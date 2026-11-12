import time
import threading
from collections import deque
from datetime import datetime

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
)
from rich.live import Live

ASCII_BANNER = r"""
  ___ _   _ _____ _____ ____  _____ ____ _____   ____      _  _____ _____ 
 |_ _| \ | |_   _| ____|  _ \| ____/ ___|_   _| |  _ \    / \|_   _| ____|
  | ||  \| | | | |  _| | |_) |  _| \___ \ | |   | |_) |  / _ \ | | |  _|  
  | || |\  | | | | |___|  _ <| |___ ___) || |   |  _ <  / ___ \| | | |___ 
 |___|_| \_| |_| |_____|_| \_\_____|____/ |_|   |_| \_\/_/   \_\_| |_____|
                 ____ _   _    _    _   _  ____ _____ 
                / ___| | | |  / \  | \ | |/ ___| ____|
               | |   | |_| | / _ \ |  \| | |  _|  _|  
               | |___|  _  |/ ___ \| |\  | |_| | |___ 
                \____|_| |_/_/   \_\_| \_|\____|_____|
"""


class InterestRateTUI:
    """Visually rich TUI for Finflux Interest Rate Change batch automation."""

    def __init__(
        self,
        total_clients: int,
        batch_size: int,
        total_batches: int,
        dry_run: bool = False,
        test_one: bool = False,
    ):
        self.console = Console()
        self.total_clients = total_clients
        self.batch_size = batch_size
        self.total_batches = total_batches
        self.dry_run = dry_run
        self.test_one = test_one
        self._lock = threading.Lock()

        self.stats = {
            "total": 0,
            "completed": 0,
            "already_active": 0,
            "approved": 0,
            "review": 0,
            "failed": 0,
        }
        self.events = deque(maxlen=7)
        self.start_time = None
        self.live = None

        self.prog = Progress(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            expand=True,
        )
        self.overall_task = self.prog.add_task(
            "[bold cyan]Overall Progress[/bold cyan]",
            total=max(total_clients, 1),
        )
        self.batch_task = self.prog.add_task(
            f"[bold magenta]Batch 1/{max(total_batches, 1)}[/bold magenta]",
            total=1,
        )

    def display_header(
        self,
        input_file: str,
        output_file: str,
        workers: int,
        product_id: int,
        today_str: str,
    ):
        banner_text = Text(ASCII_BANNER.strip("\n"), style="bold cyan", justify="center")
        header_panel = Panel(
            banner_text,
            title="[bold green]FINFLUX CORE BANKING AUTOMATION[/bold green]",
            subtitle="[bold yellow]% % % INTEREST RATE CHANGE & RECURRING DEPOSIT UPDATER (12% APR) % % %[/bold yellow]",
            border_style="bright_blue",
            padding=(0, 1),
        )
        self.console.print(header_panel)

        grid = Table.grid(expand=True, padding=(0, 2))
        grid.add_column(style="cyan", justify="right")
        grid.add_column(style="white", justify="left")
        grid.add_column(style="cyan", justify="right")
        grid.add_column(style="white", justify="left")

        mode_str = (
            "[bold yellow]DRY RUN (Simulated)[/bold yellow]"
            if self.dry_run
            else "[bold green]LIVE PRODUCTION[/bold green]"
        )
        if self.test_one:
            mode_str += " [italic](Test 1 Client)[/italic]"

        grid.add_row(
            "[bold]Input File:[/bold]",
            input_file,
            "[bold]Output File:[/bold]",
            output_file,
        )
        grid.add_row(
            "[bold]Interest Rate:[/bold]",
            "[bold yellow]12.0% APR (Fixed Rate)[/bold yellow]",
            "[bold]CS Product ID:[/bold]",
            f"{product_id} (Finflux Staging)",
        )
        grid.add_row(
            "[bold]Workers / Threads:[/bold]",
            f"{workers} concurrent workers",
            "[bold]Batch Config:[/bold]",
            f"{self.batch_size} clients/batch ({self.total_batches} total)",
        )
        grid.add_row(
            "[bold]Execution Mode:[/bold]",
            mode_str,
            "[bold]Effective Date:[/bold]",
            today_str,
        )

        config_panel = Panel(
            grid,
            title="[bold]Execution Configuration & Settings[/bold]",
            border_style="blue",
        )
        self.console.print(config_panel)

    def start(self):
        self.start_time = time.time()
        self.live = Live(
            self._render_view(),
            console=self.console,
            refresh_per_second=4,
            transient=True,
        )
        self.live.start()

    def start_batch(self, batch_num: int, batch_count: int):
        with self._lock:
            desc = f"[bold magenta]Batch {batch_num}/{self.total_batches:<2}[/bold magenta]"
            self.prog.reset(
                self.batch_task,
                description=desc,
                total=batch_count,
                completed=0,
            )
            if self.live:
                self.live.update(self._render_view())
    def record_client(self, row: dict):
        with self._lock:
            client_id = row.get("clientId")
            savings_id = row.get("savingsId")
            res = row.get("processing_result", "UNKNOWN")

            if res == "COMPLETED":
                self.stats["completed"] += 1
                badge = "[bold green]COMPLETED[/bold green]"
                if row.get("create_status") == "SUCCESS":
                    details = "New CS account created -> Approved -> Activated (12% APR)"
                else:
                    details = "Existing account approved & activated (12% APR)"
            elif res == "ALREADY_ACTIVATED":
                self.stats["already_active"] += 1
                badge = "[bold blue]ALREADY ACTIVE[/bold blue]"
                details = "CS account already active (status 300)"
            elif res == "FAILED_NO_STAFF_ID":
                self.stats["failed"] += 1
                badge = "[bold red]NO STAFF ID[/bold red]"
                details = "No staff_id found in m_client table"
            elif res == "MULTIPLE_ACCOUNTS_REVIEW":
                self.stats["review"] += 1
                badge = "[bold yellow]MULTI ACCTS[/bold yellow]"
                details = "Multiple CS accounts found for client"
            elif res in ("REVIEW_CREATE_RESPONSE", "UNKNOWN_STATUS_REVIEW"):
                self.stats["review"] += 1
                badge = "[bold yellow]NEEDS REVIEW[/bold yellow]"
                details = (row.get("create_error") or "Manual review required")[:45]
            elif res == "CHECK_DATABASE_BEFORE_RETRY":
                self.stats["review"] += 1
                badge = "[bold yellow]CHECK DB[/bold yellow]"
                details = (row.get("create_error") or "Timeout - verify DB before retry")[:45]
            elif res == "DRY_RUN":
                self.stats["completed"] += 1
                badge = "[bold cyan]DRY RUN[/bold cyan]"
                details = "Simulated client processing (dry run)"
            elif "FAILED" in res or res in ("UNEXPECTED_ERROR", "WORKER_FAILED"):
                self.stats["failed"] += 1
                badge = "[bold red]FAILED[/bold red]"
                err = (
                    row.get("create_error")
                    or row.get("approve_error")
                    or row.get("activate_error")
                    or "Error occurred"
                )
                details = str(err)[:45]
            else:
                self.stats["review"] += 1
                badge = "[bold yellow]UNKNOWN[/bold yellow]"
                details = str(res)[:45]

            self.stats["total"] += 1

            self.events.append(
                {
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "client_id": client_id,
                    "savings_id": str(savings_id) if savings_id is not None else "-",
                    "badge": badge,
                    "details": details,
                }
            )

            self.prog.advance(self.overall_task, 1)
            self.prog.advance(self.batch_task, 1)

            if self.live:
                self.live.update(self._render_view())

    def log_save(self, saved_count: int, total_count: int):
        with self._lock:
            self.events.append(
                {
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "client_id": "---",
                    "savings_id": "---",
                    "badge": "[bold magenta]REPORT SAVED[/bold magenta]",
                    "details": f"Auto-saved progress: {saved_count}/{total_count} clients",
                }
            )
            if self.live:
                self.live.update(self._render_view())

    def log_pause(self, seconds: int, next_batch: int, total_batches: int):
        with self._lock:
            self.events.append(
                {
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "client_id": "---",
                    "savings_id": "---",
                    "badge": "[bold yellow]BATCH PAUSE[/bold yellow]",
                    "details": f"Waiting {seconds}s before starting Batch {next_batch}/{total_batches}...",
                }
            )
            if self.live:
                self.live.update(self._render_view())

    def stop(self):
        if self.live:
            self.live.stop()
            self.live = None

    def _render_view(self):
        # 1. Progress Panel
        p_prog = Panel(
            self.prog,
            title="[bold cyan]Execution Progress[/bold cyan]",
            border_style="cyan",
        )

        # 2. Stats Panel
        tbl_stats = Table(expand=True, box=None, padding=(0, 1))
        tbl_stats.add_column("[bold green]✓ Activated (New)[/bold green]", justify="center")
        tbl_stats.add_column("[bold blue]ℹ Already Active[/bold blue]", justify="center")
        tbl_stats.add_column("[bold yellow]⚠ Needs Review[/bold yellow]", justify="center")
        tbl_stats.add_column("[bold red]✗ Failed / Error[/bold red]", justify="center")
        tbl_stats.add_column("[bold white]Processed Total[/bold white]", justify="center")

        tbl_stats.add_row(
            f"[bold green]{self.stats['completed']}[/bold green]",
            f"[bold blue]{self.stats['already_active']}[/bold blue]",
            f"[bold yellow]{self.stats['review']}[/bold yellow]",
            f"[bold red]{self.stats['failed']}[/bold red]",
            f"[bold white]{self.stats['total']} / {self.total_clients}[/bold white]",
        )
        p_stats = Panel(
            tbl_stats,
            title="[bold green]Live Metrics[/bold green]",
            border_style="green",
        )

        # 3. Activity Table
        tbl_act = Table(expand=True, padding=(0, 1))
        tbl_act.add_column("Time", style="dim cyan", width=10)
        tbl_act.add_column("Client ID", style="bold white", width=12)
        tbl_act.add_column("Savings ID", style="cyan", width=12)
        tbl_act.add_column("Status", width=18)
        tbl_act.add_column("Details", style="white")

        for ev in self.events:
            tbl_act.add_row(
                ev["time"],
                str(ev["client_id"]),
                str(ev["savings_id"]),
                ev["badge"],
                ev["details"],
            )

        p_act = Panel(
            tbl_act,
            title="[bold magenta]Recent Activity Feed[/bold magenta]",
            border_style="magenta",
        )

        return Group(p_prog, p_stats, p_act)

    def show_final_summary(self, output_file: str):
        elapsed = time.time() - (self.start_time or time.time())
        mins, secs = divmod(int(elapsed), 60)
        elapsed_str = f"{mins:02d}m {secs:02d}s"

        table = Table(title="Execution Statistics Breakdown", expand=True)
        table.add_column("Category", style="cyan", justify="left")
        table.add_column("Count", style="bold white", justify="right")
        table.add_column("Share", style="dim white", justify="right")
        table.add_column("Description", style="white", justify="left")

        tot = max(self.stats["total"], 1)

        rows = [
            (
                "Newly Activated",
                self.stats["completed"],
                f"{self.stats['completed']/tot*100:.1f}%",
                "Created with 12% APR chart, approved & activated",
                "green",
            ),
            (
                "Already Active",
                self.stats["already_active"],
                f"{self.stats['already_active']/tot*100:.1f}%",
                "CS Savings account already active (status 300)",
                "blue",
            ),
            (
                "Needs Review",
                self.stats["review"],
                f"{self.stats['review']/tot*100:.1f}%",
                "Multiple accounts or unexpected API response",
                "yellow",
            ),
            (
                "Failed / Error",
                self.stats["failed"],
                f"{self.stats['failed']/tot*100:.1f}%",
                "Missing staff_id, API failure, or worker exception",
                "red",
            ),
        ]

        for cat, cnt, pct, desc, col in rows:
            table.add_row(f"[{col}]{cat}[/{col}]", f"[{col}]{cnt}[/{col}]", pct, desc)

        table.add_section()
        table.add_row(
            "[bold white]TOTAL PROCESSED[/bold white]",
            f"[bold white]{self.stats['total']}[/bold white]",
            "100.0%",
            "All unique clients evaluated from input file",
        )

        footer_grid = Table.grid(expand=True, padding=(0, 2))
        footer_grid.add_column(style="bold cyan", justify="right")
        footer_grid.add_column(style="white", justify="left")
        footer_grid.add_column(style="bold cyan", justify="right")
        footer_grid.add_column(style="white", justify="left")

        speed = self.stats["total"] / max(elapsed, 0.001)

        footer_grid.add_row(
            "Output File:",
            output_file,
            "Total Elapsed Time:",
            elapsed_str,
        )
        footer_grid.add_row(
            "Status:",
            "[bold green]SUCCESS / BATCH COMPLETE[/bold green]",
            "Average Speed:",
            f"{speed:.1f} clients/sec",
        )
        footer_grid.add_row(
            "Interest Rate:",
            "[bold yellow]12.0% APR (Fixed Rate)[/bold yellow]",
            "CS Product ID:",
            "46 (Finflux Staging)",
        )

        summary_panel = Panel(
            Group(table, Text(""), footer_grid),
            title="[bold green]✔ PROCESSING FINISHED - INTEREST RATE UPDATE COMPLETE[/bold green]",
            border_style="green",
            padding=(1, 2),
        )
        self.console.print(summary_panel)

