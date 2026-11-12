import json
import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import mysql.connector
import requests

# =========================================================
# STEP 1: CONFIGURATION
# =========================================================

INPUT_FILE = "loan_list.xlsx"
OUTPUT_FILE = "created_savings_accounts.xlsx"

API_BASE_URL = (
    "https://hanastaging.finflux.io/"
    "fineract-provider/api/v1/recurringdepositaccounts"
)

# eg. 17f5fd3c-6233-4b0f-b9ba-cc3a5020121a
TOKEN = "db2f744b-6f4e-4b47-835e-03ec7e0c7f36"

# Database configuration
DB_CONFIG = {
    "host": "123.253.22.190",
    "port": 3366,
    "user": "hmf_uat_application_user",
    "password": "App@2026UAT",
    "database": "hmf_uat_finflux_db",
}

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json;charset=UTF-8",
    "fineract-platform-tenantid": "hanastaging",
}

# Existing account lookup configuration.
ACCOUNT_TABLE = "m_savings_account"
ACCOUNT_ID_COLUMN = "id"
ACCOUNT_CLIENT_ID_COLUMN = "client_id"
ACCOUNT_PRODUCT_ID_COLUMN = "product_id"
ACCOUNT_STATUS_COLUMN = "status_enum"

CS_PRODUCT_ID = 46 # Product ID of Finflux Staging Env

# FinFlux account status values
STATUS_PENDING = 100
STATUS_APPROVED = 200
STATUS_ACTIVATED = 300

# Safety settings
DRY_RUN = False
TEST_ONE_CLIENT = False

# Batch settings
BATCH_SIZE = 500
BATCH_PAUSE_SECONDS = 1

MAX_WORKERS = 15

TODAY = date.today().strftime("%d %B %Y")

# TODAY = "26 October 2026"

REPORT_COLUMNS = [
    "officeId",
    "clientId",
    "loan_id",
    "savingsId",
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

SAVE_INTERVAL = 100

# =========================================================
# STEP 2: REPORT FUNCTIONS
# =========================================================

def new_report_row(client_id, loan_id=None):
    return {
        "officeId": None,
        "clientId": int(client_id),
        "loan_id": int(loan_id) if loan_id is not None else None,
        "savingsId": None,
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


def save_report(report):
    output_df = pd.DataFrame(
        list(report.values()),
        columns=REPORT_COLUMNS,
    )
    output_df.to_excel(OUTPUT_FILE, index=False)

# =========================================================
# STEP 3: DATABASE LOOKUP
# =========================================================

def get_existing_cs_account(cursor, client_id):
    """
    Return all CS accounts matching this client and product.   
    """
    identifiers = [
        ACCOUNT_TABLE,
        ACCOUNT_ID_COLUMN,
        ACCOUNT_CLIENT_ID_COLUMN,
        ACCOUNT_PRODUCT_ID_COLUMN,
        ACCOUNT_STATUS_COLUMN,
    ]

    for identifier in identifiers:
        if not identifier.replace("_", "").isalnum():
            raise ValueError(f"Invalid SQL identifier: {identifier}")

    query = f"""
        SELECT
            `{ACCOUNT_ID_COLUMN}`,
            `{ACCOUNT_STATUS_COLUMN}`
        FROM `{ACCOUNT_TABLE}`
        WHERE `{ACCOUNT_CLIENT_ID_COLUMN}` = %s
          AND `{ACCOUNT_PRODUCT_ID_COLUMN}` = %s
    """

    cursor.execute(query, (int(client_id), int(CS_PRODUCT_ID)))
    return cursor.fetchall()

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
# STEP 6: GENERIC API REQUEST FUNCTION
# =========================================================

def call_api(method, url, payload=None):
    try:
        response = requests.request(
            method=method,
            url=url,
            headers=HEADERS,
            json=payload,
            timeout=(10, 60),
        )

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

    print(f"Calling {command.upper()} for savingsId={savings_id}")

    result = call_api("POST", url, payload)

    if result["success"]:
        print(
            f"{command.upper()} succeeded "
            f"(HTTP {result['http_status']})."
        )
    else:
        print(f"{command.upper()} failed: {result['error']}")

    return result


# =========================================================
# STEP 8: CREATE ACCOUNT
# =========================================================

def create_cs_account(client_id, staff_id):
    payload = build_payload(client_id, staff_id)

    if DRY_RUN:
        print(f"[Client {client_id}] DRY RUN: CREATE request body:")
        print(json.dumps(payload, indent=2))
        return {
            "success": False,
            "dry_run": True,
            "http_status": None,
            "data": None,
            "error": "DRY_RUN: account was not created.",
        }

    print(f"[Client {client_id}] Creating CS Savings Account...")
    return call_api("POST", API_BASE_URL, payload)


# =========================================================
# STEP 9: PROCESS APPROVAL
# =========================================================

def approve_account(row):
    savings_id = row["savingsId"]

    if DRY_RUN:
        print(
            f"[Client {row['clientId']}] DRY RUN: "
            f"would APPROVE savingsId={savings_id}"
        )
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
    savings_id = row["savingsId"]

    if DRY_RUN:
        print(
            f"[Client {row['clientId']}] DRY RUN: "
            f"would ACTIVATE savingsId={savings_id}"
        )
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

def process_client(client_id, loan_id, existing_row):
    """
    Process one client.

    Every execution checks the database first:
      - No account: Create -> Approve -> Activate
      - Status 100: Approve -> Activate
      - Status 200: Activate
      - Status 300: Skip

    """
    row = dict(existing_row)
    row["clientId"] = int(client_id)

    if row["loan_id"] is None:
        row["loan_id"] = loan_id

    conn = None
    cursor = None

    print("\n" + "=" * 65)
    print(f"[Worker] Processing client_id={client_id}, loan_id={loan_id}")

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # -------------------------------------------------
        # A. GET STAFF ID
        # -------------------------------------------------
        cursor.execute(
            """
            SELECT staff_id
            FROM m_client
            WHERE id = %s
            """,
            (int(client_id),),
        )

        client_result = cursor.fetchone()

        if client_result is None or client_result[0] is None:
            row["processing_result"] = "FAILED_NO_STAFF_ID"
            row["create_status"] = "FAILED"
            row["create_error"] = "No staff_id found in m_client."
            print(f"[Client {client_id}] No staff_id found. Skipping.")
            return row

        staff_id = int(client_result[0])
        print(f"[Client {client_id}] staff_id={staff_id}")

        # -------------------------------------------------
        # B. CHECK EXISTING CS ACCOUNT
        # -------------------------------------------------
        existing_accounts = get_existing_cs_account(cursor, client_id)

        if len(existing_accounts) > 1:
            row["processing_result"] = "MULTIPLE_ACCOUNTS_REVIEW"
            row["create_status"] = "REVIEW_REQUIRED"
            row["create_error"] = (
                "Multiple CS Savings Accounts found for this client "
                f"and product ID {CS_PRODUCT_ID}. Manual review required. "
                f"Matching records: {existing_accounts}"
            )
            print(f"[Client {client_id}] Multiple matching accounts found.")
            return row

        if len(existing_accounts) == 1:
            savings_id, account_status = existing_accounts[0]
            savings_id = int(savings_id)
            account_status = int(account_status)

            row["savingsId"] = savings_id
            row["existing_account_status"] = account_status

            print(
                f"[Client {client_id}] Existing CS account found: "
                f"savingsId={savings_id}, status={account_status}"
            )

            row["create_status"] = "SKIPPED_EXISTING"

        else:
            # -------------------------------------------------
            # C. NO EXISTING ACCOUNT: CREATE ONCE
            # -------------------------------------------------
            print(f"[Client {client_id}] No existing CS account found.")

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
                    row["officeId"] = data["officeId"]
                    row["clientId"] = data["clientId"]
                    row["savingsId"] = int(data["savingsId"])
                    row["create_status"] = "SUCCESS"
                    row["create_error"] = None

                    # A successfully created account starts at Pending (100).
                    account_status = STATUS_PENDING

                    print(
                        f"[Client {client_id}] Account created successfully. "
                        f"savingsId={row['savingsId']}"
                    )
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
            print(
                f"[Client {client_id}] Account status is 100 (Pending). "
                "Approval is required."
            )

            if not approve_account(row):
                row["processing_result"] = "APPROVAL_FAILED"
                return row

            account_status = STATUS_APPROVED

        elif account_status == STATUS_APPROVED:
            row["approve_status"] = "SKIPPED_ALREADY_APPROVED"
            print(
                f"[Client {client_id}] Account status is 200 (Approved). "
                "Skipping approval."
            )

        elif account_status == STATUS_ACTIVATED:
            row["approve_status"] = "SKIPPED_ALREADY_APPROVED"
            row["activate_status"] = "SKIPPED_ALREADY_ACTIVATED"
            row["processing_result"] = "ALREADY_ACTIVATED"
            print(
                f"[Client {client_id}] Account status is 300 (Activated). "
                "No further action required."
            )
            return row

        else:
            row["processing_result"] = "UNKNOWN_STATUS_REVIEW"
            row["create_error"] = (
                f"Unsupported account status: {account_status}. "
                "Manual review required."
            )
            print(
                f"[Client {client_id}] Unsupported account status "
                f"{account_status}. Skipping."
            )
            return row

        # -------------------------------------------------
        # E. ACTIVATE IF STATUS IS 200
        # -------------------------------------------------
        if account_status == STATUS_APPROVED:
            print(
                f"[Client {client_id}] Account status is 200 (Approved). "
                "Activation is required."
            )

            if not activate_account(row):
                row["processing_result"] = "ACTIVATION_FAILED"
                return row

            row["processing_result"] = "COMPLETED"
            print(f"[Client {client_id}] CS Savings Account activated.")

        return row

    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        print(f"[Client {client_id}] UNEXPECTED ERROR: {error_text}")

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
    if not TOKEN:
        raise ValueError("Finflux API token is missing. Please configure TOKEN before running.")

    required_db_values = ["host", "user", "password", "database"]
    missing_db_values = [
        name for name in required_db_values
        if not DB_CONFIG.get(name)
        or str(DB_CONFIG.get(name)).startswith("YOUR_")
    ]

    if missing_db_values:
        raise ValueError(
            "Please configure these database values: "
            + ", ".join(missing_db_values)
        )

    if MAX_WORKERS < 1:
        raise ValueError("MAX_WORKERS must be at least 1.")

    if BATCH_SIZE < 1:
        raise ValueError("BATCH_SIZE must be at least 1.")

    HEADERS["Authorization"] = f"Bearer {TOKEN}"

    df = pd.read_excel(INPUT_FILE)

    required_columns = ["client_id", "loan_id"]
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(
                f"'{column}' not found in input file. "
                f"Available columns: {df.columns.tolist()}"
            )

    df["client_id"] = pd.to_numeric(df["client_id"], errors="coerce")
    df = df.dropna(subset=["client_id"]).copy()
    df["client_id"] = df["client_id"].astype("int64")
    df = df.drop_duplicates(subset=["client_id"])

    print("Unique clients in input file:", len(df))

    if TEST_ONE_CLIENT:
        df = df.head(1)
        print("TEST_ONE_CLIENT is ON. Only the first client will be processed.")

    if DRY_RUN:
        print("DRY_RUN is ON. No API requests will be sent.")

    # report = load_previous_report()
    report = {}

    total_clients = len(df)
    total_batches = (
        total_clients + BATCH_SIZE - 1
    ) // BATCH_SIZE

    print(f"Total clients: {total_clients}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Total batches: {total_batches}")
    print(f"MAX_WORKERS: {MAX_WORKERS}")

    if MAX_WORKERS == 1:
        print("Execution mode: SEQUENTIAL")
    else:
        print(f"Execution mode: PARALLEL (up to {MAX_WORKERS} clients)")

    for batch_number, batch_start in enumerate(
        range(0, total_clients, BATCH_SIZE),
        start=1,
    ):
        batch_df = df.iloc[batch_start:batch_start + BATCH_SIZE]

        print("\n" + "#" * 65)
        print(f"STARTING BATCH {batch_number}/{total_batches}")
        print(f"Clients in this batch: {len(batch_df)}")
        print("#" * 65)

        jobs = []

        for _, loan in batch_df.iterrows():
            client_id = int(loan["client_id"])

            loan_id_value = loan["loan_id"]
            loan_id = (
                int(loan_id_value)
                if pd.notna(loan_id_value)
                else None
            )

            key = str(client_id)

            if key not in report:
                report[key] = new_report_row(client_id, loan_id)

            base_row = new_report_row(client_id, loan_id)
            jobs.append((client_id, loan_id, base_row))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_client = {
                executor.submit(
                    process_client,
                    client_id,
                    loan_id,
                    base_row,
                ): client_id
                for client_id, loan_id, base_row in jobs
            }

            completed = 0

            for future in as_completed(future_to_client):
                client_id = future_to_client[future]
                key = str(client_id)

                try:
                    updated_row = future.result()
                except Exception as exc:
                    updated_row = new_report_row(client_id)
                    updated_row["processing_result"] = "WORKER_FAILED"
                    updated_row["create_status"] = "FAILED"
                    updated_row["create_error"] = (
                        f"Worker failure: {type(exc).__name__}: {exc}"
                    )

                report[key] = updated_row

                # Save after each completed client.
                completed += 1

                # Save periodically instead of after every client.
                if completed % SAVE_INTERVAL == 0:
                    save_report(report)
                    print(f"[Report] Saved progress: {len(report)}/{total_clients} clients.")

                print(
                    f"[Progress] Batch {batch_number}/{total_batches}: "
                    f"{completed}/{len(jobs)} clients completed. "
                    f"Latest client: {client_id}"
                )
        # Save after every completed batch.
        save_report(report)

        print(f"\nBATCH {batch_number}/{total_batches} COMPLETED")

        if batch_number < total_batches:
            print(
                f"Waiting {BATCH_PAUSE_SECONDS} seconds "
                "before the next batch..."
            )
            time.sleep(BATCH_PAUSE_SECONDS)

    save_report(report)

    print("\n" + "=" * 65)
    print("PROCESSING FINISHED")
    print(f"Report saved to: {OUTPUT_FILE}")
    print(f"Total clients in input: {total_clients}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Total batches: {total_batches}")
    print(f"MAX_WORKERS: {MAX_WORKERS}")
    print(f"Date used: {TODAY}")
    print(f"Dry run: {DRY_RUN}")
    print(f"Test one client: {TEST_ONE_CLIENT}")


if __name__ == "__main__":
    main()
