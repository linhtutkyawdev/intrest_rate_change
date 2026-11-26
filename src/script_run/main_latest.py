import argparse
import io
import json
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from datetime import date
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
import requests
import threading
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

_thread_local = threading.local()

from rich.console import Console
from rich.panel import Panel

try:
    from script_run.tui import InterestRateTUI
except ImportError:
    from tui import InterestRateTUI


# =========================================================
# STEP 1: CONFIGURATION (Loaded from .env with defaults)
# =========================================================

# File & Checkpoint paths
INPUT_FILE = os.getenv("INPUT_FILE", "loan_list.xlsx")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "created_savings_accounts.xlsx")
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "savings_accounts_checkpoint.jsonl")

# S3 Output Directory (e.g. s3://uat.hanamicrofinance.net/default/intrest_rate_change/)
# If empty and INPUT_FILE is an s3:// URL, defaults to the folder of INPUT_FILE
S3_OUTPUT_DIR = os.getenv("S3_OUTPUT_DIR", "").strip()

# AWS S3 credentials override (if empty, boto3 falls back to ~/.aws or IAM roles)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "").strip()
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "")).strip()


def is_s3_path(path: str) -> bool:
    """Check if the given path is an S3 URI."""
    return bool(path and path.strip().startswith("s3://"))


def get_local_checkpoint_path(checkpoint_file=CHECKPOINT_FILE) -> str:
    """
    Get the local file path to append checkpoint lines to.
    If CHECKPOINT_FILE is an S3 URI (s3://bucket/key), returns its basename locally.
    """
    cleaned = (checkpoint_file or "").strip()
    if is_s3_path(cleaned):
        return os.path.basename(urlparse(cleaned).path)
    return cleaned or "savings_accounts_checkpoint.jsonl"


def get_local_output_path(output_file=OUTPUT_FILE) -> str:
    """
    Get the local file path to write Excel output to.
    If OUTPUT_FILE is an S3 URI (s3://bucket/key), returns its basename locally.
    """
    cleaned = (output_file or "").strip()
    if is_s3_path(cleaned):
        return os.path.basename(urlparse(cleaned).path)
    return cleaned or "created_savings_accounts.xlsx"


LOCAL_CHECKPOINT_FILE = get_local_checkpoint_path(CHECKPOINT_FILE)
LOCAL_OUTPUT_FILE = get_local_output_path(OUTPUT_FILE)

# Resume settings
RESUME_IF_EXISTS = os.getenv("RESUME_IF_EXISTS", "True").strip().lower() in ("true", "1", "yes")

# Terminal statuses that should NOT be re-processed on resume.
# Every other status (failed, timeout, review, not attempted) will be retried on resume.
COMPLETED_RESULTS = {
    "COMPLETED",
    "ALREADY_ACTIVATED",
}

# OAuth & Finflux API configuration
FINERACT_OAUTH_URL = os.getenv(
    "FINERACT_OAUTH_URL",
    "https://hanastaging.finflux.io/fineract-provider/api/oauth/token",
)
FINERACT_TENANT_ID = os.getenv("FINERACT_TENANT_ID", "hanastaging")
FINERACT_USERNAME = os.getenv("FINERACT_USERNAME", "autoteam")
FINERACT_PASSWORD = os.getenv("FINERACT_PASSWORD", "AutoTeam@#2026")
FINERACT_CLIENT_ID = os.getenv("FINERACT_CLIENT_ID", "community-app")
FINERACT_CLIENT_SECRET = os.getenv("FINERACT_CLIENT_SECRET", "123")
FINERACT_GRANT_TYPE = os.getenv("FINERACT_GRANT_TYPE", "password")

API_BASE_URL = os.getenv(
    "FINERACT_API_BASE_URL",
    os.getenv(
        "API_BASE_URL",
        "https://hanastaging.finflux.io/fineract-provider/api/v1/recurringdepositaccounts",
    ),
)

# Optional static token override; if empty, token is obtained automatically via OAuth
TOKEN = os.getenv("FINERACT_TOKEN", os.getenv("TOKEN", "")).strip()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "123.253.22.190"),
    "port": int(os.getenv("DB_PORT", "3366")),
    "user": os.getenv("DB_USER", "hmf_uat_application_user"),
    "password": os.getenv("DB_PASSWORD", "App@2026UAT"),
    "database": os.getenv("DB_DATABASE", "hmf_uat_finflux_db"),
}

HEADERS = {
    "Authorization": f"Bearer {TOKEN}" if TOKEN else "",
    "Content-Type": "application/json;charset=UTF-8",
    "fineract-platform-tenantid": FINERACT_TENANT_ID,
}

# Existing account lookup configuration.
ACCOUNT_TABLE = os.getenv("ACCOUNT_TABLE", "m_savings_account")
ACCOUNT_ID_COLUMN = os.getenv("ACCOUNT_ID_COLUMN", "id")
ACCOUNT_CLIENT_ID_COLUMN = os.getenv("ACCOUNT_CLIENT_ID_COLUMN", "client_id")
ACCOUNT_PRODUCT_ID_COLUMN = os.getenv("ACCOUNT_PRODUCT_ID_COLUMN", "product_id")
ACCOUNT_STATUS_COLUMN = os.getenv("ACCOUNT_STATUS_COLUMN", "status_enum")

CS_PRODUCT_ID = int(os.getenv("CS_PRODUCT_ID", "46"))  # Product ID of Finflux Staging Env

# FinFlux account status values
STATUS_PENDING = int(os.getenv("STATUS_PENDING", "100"))
STATUS_APPROVED = int(os.getenv("STATUS_APPROVED", "200"))
STATUS_ACTIVATED = int(os.getenv("STATUS_ACTIVATED", "300"))

# Safety settings
DRY_RUN = os.getenv("DRY_RUN", "False").strip().lower() in ("true", "1", "yes")
TEST_ONE_CLIENT = os.getenv("TEST_ONE_CLIENT", "False").strip().lower() in ("true", "1", "yes")

if DRY_RUN:
    COMPLETED_RESULTS.add("DRY_RUN")

# Batch settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "300"))
BATCH_PAUSE_SECONDS = int(os.getenv("BATCH_PAUSE_SECONDS", "1"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "15"))
SAVE_INTERVAL = int(os.getenv("SAVE_INTERVAL", "300"))

TODAY = date.today().strftime("%d %B %Y")

REPORT_COLUMNS = [
    "office_id",
    "client_id",
    "csavings14_id",
    "csavings12_id",
    "existing_account_status",
    "processing_result",
    "create_status",
    "create_http_status",
    "create_error",
    "approve_status",
    "approve_http_status",
    "approve_error",
    "activate_status",
    "activate_http_status",
    "activate_error",
]

NOT_ATTEMPTED = "NOT_ATTEMPTED"

SAVE_INTERVAL = 300

# =========================================================
# STEP 2: REPORT & CHECKPOINT FUNCTIONS
# =========================================================

_checkpoint_lock = threading.Lock()


def format_id(val):
    if val is None or pd.isna(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return val


def new_report_row(client_id, csavings14_id=None):
    return {
        "office_id": None,
        "client_id": int(client_id),
        "csavings14_id": format_id(csavings14_id),
        "csavings12_id": None,
        "existing_account_status": None,
        "processing_result": NOT_ATTEMPTED,
        "create_status": NOT_ATTEMPTED,
        "create_http_status": None,
        "create_error": None,
        "approve_status": NOT_ATTEMPTED,
        "approve_http_status": None,
        "approve_error": None,
        "activate_status": NOT_ATTEMPTED,
        "activate_http_status": None,
        "activate_error": None,
    }


def append_to_checkpoint(row, checkpoint_path=LOCAL_CHECKPOINT_FILE):
    """
    Append a single processed client record as JSON to the local checkpoint file.
    Fast (<0.1ms), thread-safe, and crash-proof for 40k+ volume.
    """
    line = json.dumps(row) + "\n"
    with _checkpoint_lock:
        with open(checkpoint_path, "a", encoding="utf-8") as f:
            f.write(line)


def load_checkpoint(checkpoint_path=LOCAL_CHECKPOINT_FILE):
    """
    Load previous progress from the JSONL checkpoint file.
    If the local checkpoint does not exist or is empty, checks and downloads
    from S3 (if CHECKPOINT_FILE is an S3 URI or if S3 sync is configured).
    Returns:
        report (dict): {str(client_id): row_dict} (latest state per client)
        completed_client_ids (set): set of client_id ints to skip (COMPLETED / ALREADY_ACTIVATED)
    """
    # If local checkpoint does not exist or is empty, try downloading from S3
    if not os.path.exists(checkpoint_path) or os.path.getsize(checkpoint_path) == 0:
        s3_cp_uri = get_checkpoint_s3_uri()
        if s3_cp_uri:
            download_s3_file_if_exists(s3_cp_uri, checkpoint_path)

    if not os.path.exists(checkpoint_path):
        return {}, set()

    report = {}
    completed_ids = set()

    try:
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    cid = row.get("client_id")
                    if cid is not None:
                        cid = int(cid)
                        report[str(cid)] = row
                except json.JSONDecodeError:
                    continue

        for str_cid, row in report.items():
            if row.get("processing_result") in COMPLETED_RESULTS:
                completed_ids.add(int(str_cid))

        return report, completed_ids
    except Exception as exc:
        print(f"Warning: Failed to load checkpoint from '{checkpoint_path}': {exc}")
        return {}, set()


def load_from_excel_fallback(output_file=LOCAL_OUTPUT_FILE, checkpoint_path=LOCAL_CHECKPOINT_FILE):
    """
    Fallback if checkpoint JSONL does not exist but previous Excel output file exists.
    Also checks S3 if local Excel does not exist.
    Populates report, identifies completed IDs, and initializes the JSONL checkpoint.
    """
    if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
        s3_out_uri = get_output_s3_uri()
        if s3_out_uri:
            download_s3_file_if_exists(s3_out_uri, output_file)

    if not os.path.exists(output_file):
        return {}, set()

    try:
        df_prev = pd.read_excel(output_file)
        if "client_id" not in df_prev.columns:
            return {}, set()

        df_prev["client_id"] = pd.to_numeric(df_prev["client_id"], errors="coerce")
        df_prev = df_prev.dropna(subset=["client_id"])
        df_prev["client_id"] = df_prev["client_id"].astype("int64")

        report = {}
        completed_ids = set()

        for row in df_prev.to_dict(orient="records"):
            cleaned_row = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            cid = int(cleaned_row["client_id"])
            report[str(cid)] = cleaned_row
            if cleaned_row.get("processing_result") in COMPLETED_RESULTS:
                completed_ids.add(cid)

        # Write to checkpoint file for fast subsequent resumes
        with _checkpoint_lock:
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                for row in report.values():
                    f.write(json.dumps(row) + "\n")

        return report, completed_ids
    except Exception as exc:
        print(f"Warning: Failed to load previous Excel file '{output_file}': {exc}")
        return {}, set()


def is_s3_path(path: str) -> bool:
    """Check if the given path is an S3 URI."""
    return bool(path and path.strip().startswith("s3://"))


def get_s3_client():
    """
    Get a boto3 S3 client.
    Uses AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from .env if provided;
    otherwise falls back to default credentials (e.g. ~/.aws or IAM roles).
    """
    import boto3

    kwargs = {}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            kwargs["aws_session_token"] = AWS_SESSION_TOKEN
    if AWS_DEFAULT_REGION:
        kwargs["region_name"] = AWS_DEFAULT_REGION

    session = boto3.Session(**kwargs)
    return session.client("s3")


def get_effective_s3_output_dir(input_file=INPUT_FILE, s3_output_dir=S3_OUTPUT_DIR) -> str:
    """
    Determine the S3 output directory.
    If S3_OUTPUT_DIR is explicitly configured, use it.
    Otherwise, if INPUT_FILE is an S3 URI, default to its parent folder.
    Returns empty string if S3 output is not configured and INPUT_FILE is local.
    """
    target = (s3_output_dir or "").strip()
    if target:
        return target if target.endswith("/") else f"{target}/"

    input_cleaned = (input_file or "").strip()
    if is_s3_path(input_cleaned):
        parsed = urlparse(input_cleaned)
        parent_dir = os.path.dirname(parsed.path)
        if not parent_dir.endswith("/"):
            parent_dir += "/"
        return f"s3://{parsed.netloc}{parent_dir}"

    return ""


def download_s3_file_if_exists(s3_uri: str, local_path: str) -> bool:
    """
    Download a file from S3 to a local file path if it exists.
    Returns True if downloaded successfully, False otherwise.
    """
    if not is_s3_path(s3_uri):
        return False

    try:
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            return False

        s3 = get_s3_client()
        s3.download_file(bucket, key, local_path)
        return True
    except Exception as exc:
        return False


def get_checkpoint_s3_uri(checkpoint_file=CHECKPOINT_FILE) -> str:
    """
    Resolve the S3 URI for the checkpoint file.
    If CHECKPOINT_FILE is already an s3:// URI, returns it.
    If S3 output directory is active, returns <s3_dir>/<basename>.
    Otherwise returns empty string.
    """
    cleaned = (checkpoint_file or "").strip()
    if is_s3_path(cleaned):
        return cleaned

    s3_dir = get_effective_s3_output_dir()
    if s3_dir:
        return f"{s3_dir}{os.path.basename(cleaned)}"

    return ""


def get_output_s3_uri(output_file=OUTPUT_FILE) -> str:
    """
    Resolve the S3 URI for the output Excel file.
    If OUTPUT_FILE is already an s3:// URI, returns it.
    If S3 output directory is active, returns <s3_dir>/<basename>.
    Otherwise returns empty string.
    """
    cleaned = (output_file or "").strip()
    if is_s3_path(cleaned):
        return cleaned

    s3_dir = get_effective_s3_output_dir()
    if s3_dir:
        return f"{s3_dir}{os.path.basename(cleaned)}"

    return ""


def upload_file_to_s3(local_path: str, s3_target_uri: str) -> bool:
    """
    Upload a local file to S3. Non-fatal on failure (returns False and prints warning).
    """
    if not os.path.exists(local_path):
        return False

    try:
        parsed = urlparse(s3_target_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            print(f"Warning: Invalid S3 target URI '{s3_target_uri}'.")
            return False

        s3 = get_s3_client()
        s3.upload_file(local_path, bucket, key)
        return True
    except Exception as exc:
        print(f"Warning: Failed to upload '{local_path}' to '{s3_target_uri}': {exc}")
        return False


def sync_outputs_to_s3(output_file=LOCAL_OUTPUT_FILE, checkpoint_file=LOCAL_CHECKPOINT_FILE):
    """
    Sync both local output Excel and checkpoint JSONL to the configured S3 directory if active.
    """
    s3_dir = get_effective_s3_output_dir()
    if not s3_dir:
        return

    output_filename = os.path.basename(output_file)
    checkpoint_filename = os.path.basename(checkpoint_file)

    upload_file_to_s3(output_file, f"{s3_dir}{output_filename}")
    upload_file_to_s3(checkpoint_file, f"{s3_dir}{checkpoint_filename}")


def read_excel_file(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Read an Excel file from either a local filesystem path or an S3 URI (s3://bucket/key).
    Uses boto3 to fetch object bytes into memory if an S3 URI is provided.
    """
    cleaned_path = file_path.strip()
    if is_s3_path(cleaned_path):
        parsed = urlparse(cleaned_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Invalid S3 URI '{file_path}'. Expected format: s3://<bucket>/<key>")

        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        return pd.read_excel(io.BytesIO(body), **kwargs)

    return pd.read_excel(cleaned_path, **kwargs)


def save_report(report, output_file=LOCAL_OUTPUT_FILE, sync_s3=True):
    """
    Atomically write report to Excel using a temporary file and rename.
    Guarantees that OUTPUT_FILE is never corrupted if interrupted mid-write.
    Optionally syncs output to S3 if configured.
    """
    if not report:
        return

    output_df = pd.DataFrame(
        list(report.values()),
        columns=REPORT_COLUMNS,
    )

    dir_name = os.path.dirname(os.path.abspath(output_file)) or "."
    with tempfile.NamedTemporaryFile(delete=False, dir=dir_name, suffix=".tmp.xlsx") as tmp:
        tmp_path = tmp.name

    try:
        output_df.to_excel(tmp_path, index=False)
        os.replace(tmp_path, output_file)
    except Exception as exc:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise exc

    if sync_s3:
        sync_outputs_to_s3(output_file=output_file, checkpoint_file=LOCAL_CHECKPOINT_FILE)


# =========================================================
# STEP 2.5: BACKUP LOGIC (--backup)
# =========================================================

def get_next_backup_number_local(base_dir=".") -> int:
    """Find the highest integer n among existing 'backup_n' directories and return n + 1."""
    max_n = 0
    if os.path.exists(base_dir):
        for entry in os.listdir(base_dir):
            if os.path.isdir(os.path.join(base_dir, entry)):
                m = re.match(r"^backup_(\d+)$", entry)
                if m:
                    max_n = max(max_n, int(m.group(1)))
    return max_n + 1


def get_next_backup_number_s3(s3_dir_uri: str) -> int:
    """Find the highest integer n among existing 'backup_n/' prefixes under s3_dir_uri and return n + 1."""
    parsed = urlparse(s3_dir_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    max_n = 0
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                sub_folder = cp["Prefix"][len(prefix):].rstrip("/")
                m = re.match(r"^backup_(\d+)$", sub_folder)
                if m:
                    max_n = max(max_n, int(m.group(1)))
    except Exception as exc:
        print(f"Warning: Failed to scan S3 backup prefixes: {exc}")
    return max_n + 1


def perform_backup(console=None):
    """
    Move INPUT_FILE, OUTPUT_FILE, and CHECKPOINT_FILE to an incremental backup_n folder.
    - If INPUT_FILE is an S3 URI, moves the files in S3 under <s3_parent_dir>/backup_<n>/.
      Also moves any local output/checkpoint files if present.
    - If INPUT_FILE is local, moves local files into ./backup_<n>/.
    """
    if console is None:
        console = Console()

    is_s3_input = is_s3_path(INPUT_FILE)

    if is_s3_input:
        s3_dir = get_effective_s3_output_dir()
        if not s3_dir:
            parsed = urlparse(INPUT_FILE)
            parent_dir = os.path.dirname(parsed.path)
            if not parent_dir.endswith("/"):
                parent_dir += "/"
            s3_dir = f"s3://{parsed.netloc}{parent_dir}"

        next_n = get_next_backup_number_s3(s3_dir)
        backup_folder_name = f"backup_{next_n}"
        backup_s3_prefix = f"{s3_dir}{backup_folder_name}/"

        console.print(f"[bold cyan]Backing up S3 files to [bold yellow]{backup_s3_prefix}[/bold yellow]...[/bold cyan]")

        s3 = get_s3_client()
        s3_files = [
            INPUT_FILE,
            get_output_s3_uri(),
            get_checkpoint_s3_uri(),
        ]
        # Deduplicate while preserving order
        unique_s3_files = list(dict.fromkeys(filter(bool, s3_files)))

        moved_s3_count = 0
        for s3_uri in unique_s3_files:
            try:
                p = urlparse(s3_uri)
                bucket = p.netloc
                src_key = p.path.lstrip("/")
                filename = os.path.basename(src_key)

                # Check if object exists
                s3.head_object(Bucket=bucket, Key=src_key)

                # Copy to backup location
                dst_prefix = urlparse(backup_s3_prefix).path.lstrip("/")
                dst_key = f"{dst_prefix}{filename}"
                s3.copy_object(
                    Bucket=bucket,
                    Key=dst_key,
                    CopySource={"Bucket": bucket, "Key": src_key},
                )
                s3.delete_object(Bucket=bucket, Key=src_key)
                console.print(f"  [green]✓ Moved in S3:[/green] {filename} -> {backup_folder_name}/{filename}")
                moved_s3_count += 1
            except Exception:
                pass

        # Also backup local files if they exist locally
        local_files = [
            LOCAL_CHECKPOINT_FILE,
            LOCAL_OUTPUT_FILE,
            INPUT_FILE if not is_s3_input else os.path.basename(urlparse(INPUT_FILE).path),
        ]
        local_moved_count = 0
        local_backup_dir = f"backup_{next_n}"
        for lf in set(local_files):
            if os.path.exists(lf):
                os.makedirs(local_backup_dir, exist_ok=True)
                dest = os.path.join(local_backup_dir, os.path.basename(lf))
                shutil.move(lf, dest)
                console.print(f"  [dim green]✓ Moved local file:[/dim green] {lf} -> {dest}")
                local_moved_count += 1

        if moved_s3_count == 0 and local_moved_count == 0:
            console.print("[yellow]No existing files were found to backup.[/yellow]")
        else:
            msg = (
                f"[bold green]Backup complete![/bold green]\n"
                f"S3 backup location: [bold cyan]{backup_s3_prefix}[/bold cyan] ({moved_s3_count} files)\n"
            )
            if local_moved_count:
                msg += f"Local backup folder: [bold cyan]{local_backup_dir}[/bold cyan] ({local_moved_count} files)"
            console.print(Panel(msg.strip(), title="Backup Successful", border_style="green"))

    else:
        # Local filesystem backup
        next_n = get_next_backup_number_local(".")
        backup_folder_name = f"backup_{next_n}"
        os.makedirs(backup_folder_name, exist_ok=True)

        console.print(f"[bold cyan]Backing up local files to [bold yellow]{backup_folder_name}/[/bold yellow]...[/bold cyan]")

        local_files = [INPUT_FILE, OUTPUT_FILE, CHECKPOINT_FILE]
        moved_count = 0
        for lf in set(local_files):
            if os.path.exists(lf):
                dest = os.path.join(backup_folder_name, os.path.basename(lf))
                shutil.move(lf, dest)
                console.print(f"  [green]✓ Moved:[/green] {lf} -> {dest}")
                moved_count += 1

        if moved_count == 0:
            console.print("[yellow]No existing files were found to backup.[/yellow]")
        else:
            console.print(
                Panel(
                    f"[bold green]Backup complete![/bold green]\n"
                    f"Files moved to: [bold cyan]{backup_folder_name}/[/bold cyan] ({moved_count} files)",
                    title="Backup Successful",
                    border_style="green",
                )
            )

# =========================================================
# STEP 3: DATABASE LOOKUP
# =========================================================

# def get_existing_cs_account(cursor, client_id):
#     """
#     Return all CS accounts matching this client and product.   
#     """
#     identifiers = [
#         ACCOUNT_TABLE,
#         ACCOUNT_ID_COLUMN,
#         ACCOUNT_CLIENT_ID_COLUMN,
#         ACCOUNT_PRODUCT_ID_COLUMN,
#         ACCOUNT_STATUS_COLUMN,
#     ]

#     for identifier in identifiers:
#         if not identifier.replace("_", "").isalnum():
#             raise ValueError(f"Invalid SQL identifier: {identifier}")

#     query = f"""
#         SELECT
#             `{ACCOUNT_ID_COLUMN}`,
#             `{ACCOUNT_STATUS_COLUMN}`
#         FROM `{ACCOUNT_TABLE}`
#         WHERE `{ACCOUNT_CLIENT_ID_COLUMN}` = %s
#           AND `{ACCOUNT_PRODUCT_ID_COLUMN}` = %s
#           AND `{ACCOUNT_STATUS_COLUMN}` is not %s
#     """

#     cursor.execute(query, (int(client_id), int(CS_PRODUCT_ID)), STATUS_CLOSED)
#     return cursor.fetchall()

# =========================================================
# STEP 4: EXTRACT API ERROR
# =========================================================

def extract_error(response, response_data):
    error_code = None
    error_message = None

    if isinstance(response_data, dict):
        errors = response_data.get("errors")

        if isinstance(errors, list) and errors:
            messages = []

            for error in errors:
                if isinstance(error, dict):
                    code = error.get("code")
                    if code is not None and error_code is None:
                        error_code = code

                    message = (
                        error.get("userMessage")
                        or error.get("defaultUserMessage")
                        or str(error)
                    )

                    parameter = error.get("parameterName")
                    if parameter:
                        message = f"{parameter}: {message}"

                    messages.append(str(message))
                else:
                    messages.append(str(error))

            error_message = " | ".join(messages)

        if not error_message:
            error_message = (
                response_data.get("defaultUserMessage")
                or response_data.get("userMessage")
                or response_data.get("message")
            )

        if error_code is None:
            error_code = response_data.get("code")

    if not error_message:
        error_message = response.text or "Unknown API error"

    return error_code, str(error_message)

# =========================================================
# STEP 5: CREATE REQUEST BODY
# =========================================================

def build_payload(client_id, staff_id):
    return {
        "productId": CS_PRODUCT_ID,
        "nominalAnnualInterestRate": 0,
        "withHoldTax": False,
        "autoRenewalEnabled": False,
        "interestCompoundingPeriodType": 7,
        "interestPostingPeriodType": 7,
        "interestCalculationType": 3,
        "interestCalculationDaysInYearType": 365,
        "preClosurePenalApplicable": False,
        "minDepositTerm": 10,
        "maxDepositTerm": 10,
        "minDepositTermTypeId": 2,
        "maxDepositTermTypeId": 3,
        "isMandatoryDeposit": False,
        "allowWithdrawal": False,
        "adjustAdvanceTowardsFuturePayments": False,

        "fieldOfficerId": int(staff_id),
        "mandatoryRecommendedDepositAmount": "100",

        "expectedFirstDepositOnDate": TODAY,
        "submittedOnDate": TODAY,

        "depositPeriod": "10",
        "depositPeriodFrequencyId": 3,
        "recurringFrequency": "10",
        "recurringFrequencyType": 3,

        "locale": "en",
        "dateFormat": "dd MMMM yyyy",
        "monthDayFormat": "dd MMM",

        "charges": [],
        "clientId": int(client_id),

        "charts": [
            {
                "fromDate": TODAY,
                "dateFormat": "dd MMMM yyyy",
                "locale": "en",
                "chartSlabs": [
                    {
                        "fromPeriod": 1,
                        "annualInterestRate": 12,
                        "locale": "en",
                        "incentives": [],
                        "periodType": 2,
                    }
                ],
                "isActiveChart": "true",
            }
        ],

        "isCalendarInherited": False,
    }

# =========================================================
# STEP 6: AUTHENTICATION & GENERIC API REQUEST FUNCTION
# =========================================================

_token_lock = threading.Lock()


def fetch_oauth_token():
    """
    Authenticate against Finflux OAuth endpoint and return access_token.
    Equivalent to:
    curl --location --request POST 'https://hanastaging.finflux.io/fineract-provider/api/oauth/token?username=autoteam&password=AutoTeam%40%232026&client_id=community-app&grant_type=password&client_secret=123' \
    --header 'Content-Type: application/json' \
    --header 'Fineract-Platform-TenantID: hanastaging'
    """
    params = {
        "username": FINERACT_USERNAME,
        "password": FINERACT_PASSWORD,
        "client_id": FINERACT_CLIENT_ID,
        "grant_type": FINERACT_GRANT_TYPE,
        "client_secret": FINERACT_CLIENT_SECRET,
    }
    headers = {
        "Content-Type": "application/json",
        "Fineract-Platform-TenantID": FINERACT_TENANT_ID,
    }

    response = requests.post(
        FINERACT_OAUTH_URL,
        params=params,
        headers=headers,
        timeout=30,
    )

    try:
        data = response.json()
    except ValueError:
        data = None

    if not response.ok or not isinstance(data, dict):
        raise RuntimeError(
            f"OAuth token request failed [{response.status_code}]: {response.text}"
        )

    access_token = data.get("access_token")
    if not access_token:
        raise ValueError(f"No access_token found in OAuth response: {data}")

    return access_token


def get_auth_token(force_refresh=False):
    """
    Thread-safe getter for OAuth access token.
    Fetches a new token if not present or if force_refresh is True.
    """
    global TOKEN, HEADERS
    with _token_lock:
        if force_refresh or not TOKEN:
            TOKEN = fetch_oauth_token()
            HEADERS["Authorization"] = f"Bearer {TOKEN}"
            HEADERS["fineract-platform-tenantid"] = FINERACT_TENANT_ID
        return TOKEN


def get_http_session():
    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        adapter = HTTPAdapter(
            pool_connections=1,
            pool_maxsize=1,
            max_retries=0,
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        _thread_local.session = session

    return _thread_local.session


def call_api(method, url, payload=None, retry_auth=True):
    try:
        session = get_http_session()

        if not HEADERS.get("Authorization"):
            get_auth_token()

        response = session.post(
            url, json=payload, headers=HEADERS, timeout=120
        )

        if response.status_code == 401 and retry_auth:
            # Token may have expired; refresh once and retry
            get_auth_token(force_refresh=True)
            return call_api(method, url, payload=payload, retry_auth=False)

        try:
            data = response.json()
        except ValueError:
            data = None

        if response.ok:
            return {
                "success": True,
                "http_status": response.status_code,
                "data": data,
                "error": None,
            }

        error_code, error_message = extract_error(response, data)

        return {
            "success": False,
            "http_status": response.status_code,
            "data": data,
            "error": (
                f"Error code: {error_code}; "
                f"Message: {error_message}"
            ),
        }

    except requests.exceptions.RequestException as exc:
        return {
            "success": False,
            "http_status": None,
            "data": None,
            "error": f"{type(exc).__name__}: {exc}",
        }
    
# =========================================================
# STEP 7: APPROVE OR ACTIVATE ACCOUNT
# =========================================================

def process_account_command(savings_id, command, date_field):
    url = f"{API_BASE_URL}/{int(savings_id)}?command={command}"

    payload = {
        date_field: TODAY,
        "locale": "en",
        "dateFormat": "dd MMMM yyyy",
    }

    return call_api("POST", url, payload)


# =========================================================
# STEP 8: CREATE ACCOUNT
# =========================================================

def create_cs_account(client_id, staff_id):
    payload = build_payload(client_id, staff_id)

    if DRY_RUN:
        return {
            "success": False,
            "dry_run": True,
            "http_status": None,
            "data": None,
            "error": "DRY_RUN: account was not created.",
        }

    return call_api("POST", API_BASE_URL, payload)


# =========================================================
# STEP 9: PROCESS APPROVAL
# =========================================================

def approve_account(row):
    savings_id = row["csavings12_id"]

    if DRY_RUN:
        return False

    result = process_account_command(
        savings_id,
        "approve",
        "approvedOnDate",
    )

    row["approve_http_status"] = result["http_status"]

    if result["success"]:
        row["approve_status"] = "SUCCESS"
        row["approve_error"] = None
        return True

    row["approve_status"] = (
        "CHECK_BEFORE_RETRY"
        if result["http_status"] is None
        else "FAILED"
    )
    row["approve_error"] = result["error"]
    return False


# =========================================================
# STEP 10: PROCESS ACTIVATION
# =========================================================

def activate_account(row):
    savings_id = row["csavings12_id"]

    if DRY_RUN:
        return False

    result = process_account_command(
        savings_id,
        "activate",
        "activatedOnDate",
    )

    row["activate_http_status"] = result["http_status"]

    if result["success"]:
        row["activate_status"] = "SUCCESS"
        row["activate_error"] = None
        return True

    row["activate_status"] = (
        "CHECK_BEFORE_RETRY"
        if result["http_status"] is None
        else "FAILED"
    )
    row["activate_error"] = result["error"]
    return False


# =========================================================
# STEP 11: PROCESS ONE CLIENT
# =========================================================

def process_client(pool, client_id, csavings14_id, existing_row):
    """
    Process one client.

    Every execution checks the database first:
      - No account: Create -> Approve -> Activate
      - Status 100: Approve -> Activate
      - Status 200: Activate
      - Status 300: Skip

    """
    row = dict(existing_row)
    row["client_id"] = int(client_id)

    if row.get("csavings14_id") is None:
        row["csavings14_id"] = format_id(csavings14_id)

    conn = None
    cursor = None

    try:
        conn = pool.get_connection()
        cursor = conn.cursor()
        # -------------------------------------------------
        # A. GET STAFF ID + CHECK EXISTING CS ACCOUNT
        # -------------------------------------------------
        cursor.execute(
            """
            SELECT
                mc.staff_id,
                msa.id AS savings_id,
                msa.status_enum AS account_status
            FROM m_client mc
            LEFT JOIN m_savings_account msa
                ON msa.client_id = mc.id
                AND msa.product_id = %s
                AND msa.status_enum in (100, 200, 300)
            WHERE mc.id = %s
            """,
            (CS_PRODUCT_ID, int(client_id)),
        )

        results = cursor.fetchall()

        # Client does not exist or has no staff_id
        if not results or results[0][0] is None:
            row["processing_result"] = "FAILED_NO_STAFF_ID"
            row["create_status"] = "FAILED"
            row["create_error"] = "No staff_id found in m_client."
            return row

        staff_id = int(results[0][0])

        # Extract matching savings accounts
        existing_accounts = [
            (result[1], result[2])
            for result in results
            if result[1] is not None
        ]

        if len(existing_accounts) > 1:
            row["processing_result"] = "MULTIPLE_ACCOUNTS_REVIEW"
            row["create_status"] = "REVIEW_REQUIRED"
            row["create_error"] = (
                "Multiple CS Savings Accounts found for this client "
                f"and product ID {CS_PRODUCT_ID}. Manual review required. "
                f"Matching records: {existing_accounts}"
            )
            return row

        if len(existing_accounts) == 1:
            savings_id, account_status = existing_accounts[0]
            savings_id = int(savings_id)
            account_status = int(account_status)

            row["csavings12_id"] = savings_id
            row["existing_account_status"] = account_status
            row["create_status"] = "SKIPPED_EXISTING"

        else:
            # -------------------------------------------------
            # C. NO EXISTING ACCOUNT: CREATE ONCE
            # -------------------------------------------------
            create_result = create_cs_account(client_id, staff_id)

            row["create_http_status"] = create_result["http_status"]

            if create_result.get("dry_run"):
                row["create_status"] = "DRY_RUN"
                row["processing_result"] = "DRY_RUN"
                return row

            data = create_result["data"]

            if create_result["success"] and isinstance(data, dict):
                required_fields = ["officeId", "clientId", "savingsId"]

                if all(data.get(field) is not None for field in required_fields):
                    row["office_id"] = data["officeId"]
                    row["client_id"] = data["clientId"]
                    row["csavings12_id"] = int(data["savingsId"])
                    row["create_status"] = "SUCCESS"
                    row["create_error"] = None

                    # A successfully created account starts at Pending (100).
                    account_status = STATUS_PENDING
                else:
                    row["create_status"] = "REVIEW_RESPONSE"
                    row["create_error"] = (
                        "Create API response is missing required IDs: "
                        + json.dumps(data)
                    )
                    row["processing_result"] = "REVIEW_CREATE_RESPONSE"
                    return row

            elif not create_result["success"]:
                # If a request times out, the server may still have created the account.
                row["create_status"] = (
                    "CHECK_BEFORE_RETRY"
                    if create_result["http_status"] is None
                    else "FAILED"
                )
                row["create_error"] = create_result["error"]
                row["processing_result"] = (
                    "CHECK_DATABASE_BEFORE_RETRY"
                    if create_result["http_status"] is None
                    else "CREATE_FAILED"
                )
                return row

            else:
                row["create_status"] = "REVIEW_RESPONSE"
                row["create_error"] = (
                    "Unexpected create response: " + str(data)
                )
                row["processing_result"] = "REVIEW_CREATE_RESPONSE"
                return row

        # -------------------------------------------------
        # D. APPROVE IF STATUS IS 100
        # -------------------------------------------------
        if account_status == STATUS_PENDING:
            if not approve_account(row):
                row["processing_result"] = "APPROVAL_FAILED"
                return row

            account_status = STATUS_APPROVED

        elif account_status == STATUS_APPROVED:
            row["approve_status"] = "SKIPPED_ALREADY_APPROVED"

        elif account_status == STATUS_ACTIVATED:
            row["approve_status"] = "SKIPPED_ALREADY_APPROVED"
            row["activate_status"] = "SKIPPED_ALREADY_ACTIVATED"
            row["processing_result"] = "ALREADY_ACTIVATED"
            return row

        else:
            row["processing_result"] = "UNKNOWN_STATUS_REVIEW"
            row["create_error"] = (
                f"Unsupported account status: {account_status}. "
                "Manual review required."
            )
            return row

        # -------------------------------------------------
        # E. ACTIVATE IF STATUS IS 200
        # -------------------------------------------------
        if account_status == STATUS_APPROVED:
            if not activate_account(row):
                row["processing_result"] = "ACTIVATION_FAILED"
                return row

            row["processing_result"] = "COMPLETED"

        return row

    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        row["processing_result"] = "UNEXPECTED_ERROR"

        if row["create_status"] == NOT_ATTEMPTED:
            row["create_status"] = "FAILED"
            row["create_error"] = error_text
        elif row["approve_status"] == NOT_ATTEMPTED:
            row["approve_status"] = "FAILED"
            row["approve_error"] = error_text
        elif row["activate_status"] == NOT_ATTEMPTED:
            row["activate_status"] = "FAILED"
            row["activate_error"] = error_text

        return row

    finally:
        if cursor is not None:
            cursor.close()

        if conn is not None and conn.is_connected():
            conn.close()


# =========================================================
# STEP 12: LOAD CLIENTS AND EXECUTE IN BATCHES
# =========================================================

def main():
    console = Console()

    parser = argparse.ArgumentParser(
        description="Finflux Savings Account Interest Rate Automation & Migration",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Move input, output, and checkpoint files to an incremental backup_n folder (in S3 when input is S3, or locally) and exit.",
    )
    args = parser.parse_args()

    if args.backup:
        perform_backup(console=console)
        return

    # Graceful signal handling (Ctrl+C, SIGTERM)
    shutdown_requested = threading.Event()

    def handle_signal(sig, frame):
        shutdown_requested.set()

    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except (ValueError, AttributeError):
        pass

    try:
        console.print("[dim]Authenticating with Finflux OAuth...[/dim]")
        active_token = get_auth_token()
        console.print(f"[bold green]Successfully authenticated with Finflux (Token: {active_token[:8]}...).[/bold green]")
    except Exception as exc:
        console.print(
            Panel(
                f"[bold red]Finflux OAuth authentication failed:[/bold red]\n{exc}",
                title="Authentication Error",
                border_style="red",
            )
        )
        return

    required_db_values = ["host", "user", "password", "database"]
    missing_db_values = [
        name
        for name in required_db_values
        if not DB_CONFIG.get(name) or str(DB_CONFIG.get(name)).startswith("YOUR_")
    ]

    if missing_db_values:
        console.print(
            Panel(
                f"[bold red]Missing database configuration values:[/bold red] {', '.join(missing_db_values)}",
                title="Database Configuration Error",
                border_style="red",
            )
        )
        return

    if MAX_WORKERS < 1:
        console.print("[bold red]MAX_WORKERS must be at least 1.[/bold red]")
        return

    if BATCH_SIZE < 1:
        console.print("[bold red]BATCH_SIZE must be at least 1.[/bold red]")
        return

    HEADERS["Authorization"] = f"Bearer {active_token}"
    HEADERS["fineract-platform-tenantid"] = FINERACT_TENANT_ID

    try:
        if is_s3_path(INPUT_FILE):
            console.print(f"[dim]Fetching input Excel file from S3: {INPUT_FILE}...[/dim]")
        df = read_excel_file(INPUT_FILE)
    except Exception as exc:
        console.print(
            Panel(
                f"[bold red]Failed to read input Excel file '{INPUT_FILE}':[/bold red]\n{exc}",
                title="Input File Error",
                border_style="red",
            )
        )
        return

    if "client_id" not in df.columns:
        console.print(
            Panel(
                f"'client_id' column not found in input file.\nAvailable columns: {df.columns.tolist()}",
                title="Excel Schema Error",
                border_style="red",
            )
        )
        return

    if "csavings14_id" not in df.columns:
        console.print(
            Panel(
                f"[bold yellow]'csavings14_id' column not found in input file '{INPUT_FILE}'.[/bold yellow]\n"
                f"Available columns: {df.columns.tolist()}\n"
                f"'csavings14_id' will default to None in output.",
                title="Input Schema Notice",
                border_style="yellow",
            )
        )

    df["client_id"] = pd.to_numeric(df["client_id"], errors="coerce")
    df = df.dropna(subset=["client_id"]).copy()
    df["client_id"] = df["client_id"].astype("int64")
    df = df.drop_duplicates(subset=["client_id"])

    if TEST_ONE_CLIENT:
        df = df.head(1)

    total_clients = len(df)
    if total_clients == 0:
        console.print(
            Panel(
                f"No valid client records found in '{INPUT_FILE}'.",
                title="No Data",
                border_style="yellow",
            )
        )
        return

    # Check for existing checkpoint or previous Excel report to resume
    report = {}
    completed_client_ids = set()

    if RESUME_IF_EXISTS:
        report, completed_client_ids = load_checkpoint(LOCAL_CHECKPOINT_FILE)
        if not completed_client_ids:
            report, completed_client_ids = load_from_excel_fallback(LOCAL_OUTPUT_FILE, LOCAL_CHECKPOINT_FILE)

    # Synchronize input file columns (csavings14_id) into existing report records
    # This ensures that even already-completed clients receive new/upgraded input data upon rerun
    has_cs14 = "csavings14_id" in df.columns
    cols_to_extract = ["client_id"]
    if has_cs14:
        cols_to_extract.append("csavings14_id")

    input_meta = {}
    for row_tuple in df[cols_to_extract].itertuples(index=False):
        cid = int(row_tuple[0])
        cs14 = format_id(row_tuple[1]) if has_cs14 else None
        input_meta[str(cid)] = cs14

    updated_existing = False
    for key, r in report.items():
        if key in input_meta:
            cs14 = input_meta[key]
            if cs14 is not None and r.get("csavings14_id") != cs14:
                r["csavings14_id"] = cs14
                updated_existing = True

    if updated_existing:
        with _checkpoint_lock:
            with open(LOCAL_CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                for r in report.values():
                    f.write(json.dumps(r) + "\n")
        save_report(report)

    # Filter out only completed clients. Retries any failed, review, or unattempted records!
    remaining_df = df[~df["client_id"].isin(completed_client_ids)].copy()
    clients_to_process = len(remaining_df)
    resumed_count = len(completed_client_ids)

    if clients_to_process == 0:
        console.print(
            Panel(
                f"[bold green]All {total_clients:,} clients have already been completed![/bold green]\n"
                f"Output file: '{OUTPUT_FILE}'\n"
                f"Checkpoint file: '{CHECKPOINT_FILE}'",
                title="All Records Completed",
                border_style="green",
            )
        )
        save_report(report)
        return

    total_batches = (clients_to_process + BATCH_SIZE - 1) // BATCH_SIZE

    tui = InterestRateTUI(
        total_clients=total_clients,
        batch_size=BATCH_SIZE,
        total_batches=total_batches,
        dry_run=DRY_RUN,
        test_one=TEST_ONE_CLIENT,
    )
    tui.display_header(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        workers=MAX_WORKERS,
        product_id=CS_PRODUCT_ID,
        today_str=TODAY,
        resumed_count=resumed_count,
    )

    if resumed_count > 0:
        preload_rows = [report[str(cid)] for cid in completed_client_ids if str(cid) in report]
        tui.preload_completed_stats(preload_rows)

    try:
        db_pool = MySQLConnectionPool(
            pool_name="savings_pool",
            pool_size=MAX_WORKERS,
            pool_reset_session=True,
            **DB_CONFIG,
        )
    except Exception as exc:
        console.print(
            Panel(
                f"[bold red]Failed to establish MySQL connection pool:[/bold red]\n{exc}",
                title="Database Connection Failure",
                border_style="red",
            )
        )
        return

    tui.start()

    try:
        for batch_number, batch_start in enumerate(
            range(0, clients_to_process, BATCH_SIZE),
            start=1,
        ):
            if shutdown_requested.is_set():
                break

            batch_df = remaining_df.iloc[batch_start : batch_start + BATCH_SIZE]
            batch_len = len(batch_df)

            tui.start_batch(batch_number, batch_len)

            jobs = []
            for _, client_row in batch_df.iterrows():
                client_id = int(client_row["client_id"])

                csavings14_val = client_row.get("csavings14_id") if "csavings14_id" in client_row else None
                csavings14_id = format_id(csavings14_val)

                key = str(client_id)

                if key not in report:
                    report[key] = new_report_row(client_id, csavings14_id)
                else:
                    if report[key].get("csavings14_id") is None and csavings14_id is not None:
                        report[key]["csavings14_id"] = csavings14_id

                base_row = new_report_row(client_id, csavings14_id)
                jobs.append((client_id, csavings14_id, base_row))

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_client = {
                    executor.submit(
                        process_client,
                        db_pool,
                        client_id,
                        csavings14_id,
                        base_row,
                    ): (client_id, csavings14_id)
                    for client_id, csavings14_id, base_row in jobs
                }

                completed_in_batch = 0

                for future in as_completed(future_to_client):
                    client_id, csavings14_id = future_to_client[future]
                    key = str(client_id)

                    try:
                        updated_row = future.result()
                    except Exception as exc:
                        updated_row = new_report_row(client_id, csavings14_id)
                        updated_row["processing_result"] = "WORKER_FAILED"
                        updated_row["create_status"] = "FAILED"
                        updated_row["create_error"] = (
                            f"Worker failure: {type(exc).__name__}: {exc}"
                        )

                    report[key] = updated_row
                    append_to_checkpoint(updated_row)
                    completed_in_batch += 1

                    tui.record_client(updated_row)

                    if completed_in_batch % SAVE_INTERVAL == 0:
                        save_report(report)
                        tui.log_save(len(report), total_clients)

            save_report(report)

            if shutdown_requested.is_set():
                break

            if batch_number < total_batches:
                tui.log_pause(BATCH_PAUSE_SECONDS, batch_number + 1, total_batches)
                time.sleep(BATCH_PAUSE_SECONDS)

    except KeyboardInterrupt:
        shutdown_requested.set()

    finally:
        tui.stop()

    save_report(report)

    if shutdown_requested.is_set():
        console.print(
            Panel(
                f"[bold yellow]Execution paused / interrupted.[/bold yellow]\n\n"
                f"• Checkpoint saved: [bold cyan]{len(report):,}[/bold cyan] client states recorded in '{CHECKPOINT_FILE}'.\n"
                f"• Excel report updated: [bold cyan]'{OUTPUT_FILE}'[/bold cyan].\n\n"
                f"[bold green]To resume, simply run the script again![/bold green]\n"
                f"All successfully completed records ({len(completed_client_ids):,}) will be skipped;\n"
                f"any interrupted or failed records will be automatically retried.",
                title="Execution Interrupted — Checkpoint Saved",
                border_style="yellow",
            )
        )
        return

    tui.show_final_summary(OUTPUT_FILE)


if __name__ == "__main__":
    main()
