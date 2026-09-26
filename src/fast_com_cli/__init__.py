import asyncio
import json
import math
import sys
import time
from typing import Annotated

import typer
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from fast_com_cli import core
from fast_com_cli.core import SpeedTestError, SpeedTestResult

app = typer.Typer(
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

for _stream in (sys.stdout, sys.stderr):
    try:
        enc = getattr(_stream, "encoding", "") or ""
        if enc.lower().replace("-", "") != "utf8":
            _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

console = Console()

SPARK_CHARS = (" ", "▂", "▃", "▄", "▅", "▆", "▇", "█")

PIPELINE_STEPS = [
    ("token", "Session Handshake", "Obtaining fast.com token"),
    ("targets", "Topology Discovery", "Resolving edge server endpoints"),
    ("ping", "Latency Baseline", "Probing unloaded network RTT"),
    ("download", "Download Stream", "Multi-stream saturation"),
    ("upload", "Upload Burst", "Upstream network benchmarking"),
]


def _speed_color(mbps: float) -> str:
    if mbps < 15:
        return "red"
    if mbps < 60:
        return "yellow"
    if mbps < 200:
        return "cyan"
    return "bright_green"


def _format_sparkline(history: list[float], peak: float, max_len: int = 16) -> str:
    if max_len <= 0:
        return ""
    if not history or peak <= 0:
        return " " * max_len
    padded = history[-max_len:]
    if len(padded) < max_len:
        padded = [0.0] * (max_len - len(padded)) + padded

    chars = []
    for val in padded:
        if val <= 0:
            chars.append(" ")
        else:
            ratio = min(max(val / peak, 0.0), 1.0)
            idx = min(int(ratio * (len(SPARK_CHARS) - 1)), len(SPARK_CHARS) - 1)
            chars.append(SPARK_CHARS[idx])
    return "".join(chars)


def _render_progress_bar(percentage: float, width: int = 24) -> str:
    if width <= 2:
        return ""
    clamped = max(0.0, min(100.0, percentage))
    filled_len = int(math.floor((clamped / 100.0) * width))
    empty_len = width - filled_len
    bar = "━" * filled_len
    if empty_len > 0:
        bar += "╺" + "─" * (empty_len - 1)
    return bar


def _render_summary(result: SpeedTestResult, elapsed: float, terminal_width: int) -> Panel:
    dl = result.download_mbps
    ul = result.upload_mbps
    ping = result.latency_ms
    loaded = result.loaded_latency_ms
    loc = result.client_location or {}
    servers = result.servers or []
    ip = result.client_ip or "Unknown"

    city = loc.get("city", "Local Node")
    country = loc.get("country", "")
    geo_str = f"{city}, {country}".strip(", ") if country else city

    is_compact = terminal_width < 65

    # Metric Cards
    if is_compact:
        cards_table = Table.grid(expand=True, padding=(0, 1))
        cards_table.add_column(justify="left", width=12)
        cards_table.add_column(justify="right")

        cards_table.add_row(
            Text("DOWNLOAD", style="dim"),
            Text(f"{dl:g} Mbps", style=f"bold {_speed_color(dl)}"),
        )
        if ul is not None:
            cards_table.add_row(
                Text("UPLOAD", style="dim"),
                Text(f"{ul:g} Mbps", style=f"bold {_speed_color(ul)}"),
            )
        else:
            cards_table.add_row(Text("UPLOAD", style="dim"), Text("SKIPPED", style="dim"))

        lat_display = f"{ping} ms"
        if loaded:
            lat_display += f" ({loaded} ms loaded)"
        cards_table.add_row(
            Text("LATENCY", style="dim"),
            Text(lat_display, style="bright_white"),
        )
    else:
        cards_table = Table.grid(expand=True, padding=(0, 2))
        cards_table.add_column(justify="center", ratio=1)
        cards_table.add_column(justify="center", ratio=1)
        cards_table.add_column(justify="center", ratio=1)

        dl_text = Text()
        dl_text.append("DOWNLOAD\n", style="dim")
        dl_text.append(f"{dl:g}", style=f"bold {_speed_color(dl)}")
        dl_text.append(" Mbps", style="dim")

        ul_text = Text()
        ul_text.append("UPLOAD\n", style="dim")
        if ul is not None:
            ul_text.append(f"{ul:g}", style=f"bold {_speed_color(ul)}")
            ul_text.append(" Mbps", style="dim")
        else:
            ul_text.append("SKIPPED", style="dim")

        lat_text = Text()
        lat_text.append("LATENCY\n", style="dim")
        lat_text.append(f"{ping}", style="bold bright_white")
        lat_text.append(" ms", style="dim")
        if loaded:
            lat_text.append(f"\n({loaded} ms loaded)", style="dim italic")

        cards_table.add_row(dl_text, ul_text, lat_text)

    # Topology & Session details
    meta_table = Table.grid(expand=True, padding=(0, 1))
    meta_table.add_column(style="dim", width=12)
    meta_table.add_column(style="bright_white", overflow="ellipsis")

    meta_table.add_row("Client Node", f"{geo_str}  ·  {ip}")
    srv_summary = ", ".join(servers[:2])
    if len(servers) > 2:
        srv_summary += f" (+{len(servers) - 2} more)"
    meta_table.add_row("Edge Targets", srv_summary or "fast.com CDN cluster")
    meta_table.add_row("Run Duration", f"{elapsed:.1f}s benchmark execution")

    divider_len = max(20, min(terminal_width - 8, 56))
    body = Group(
        cards_table,
        Text("\n" + "─" * divider_len + "\n", style="bright_black"),
        meta_table,
    )

    return Panel(
        body,
        title=Text(" fast.com  ·  session benchmark ", style="bold bright_white"),
        subtitle=Text(" status: completed ", style="dim green"),
        border_style="bright_black",
        box=box.ROUNDED,
        padding=(0, 1) if is_compact else (1, 2),
    )


class HarnessUIState:
    def __init__(self, download_duration: float, upload_duration: float, skip_upload: bool):
        self.download_duration = download_duration
        self.upload_duration = upload_duration
        self.skip_upload = skip_upload
        self.current_stage = "token"
        self.status_detail = "Initializing test session..."
        self.client_info: dict = {}
        self.servers: list = []
        self.current_mbps: float = 0.0
        self.peak_mbps: float = 0.0
        self.history_mbps: list[float] = []
        self.bytes_transferred: int = 0
        self.stage_start_time: float = time.perf_counter()
        self.spin_index: int = 0

    def set_stage(self, stage: str, message: str, meta: dict | None = None) -> None:
        if stage != self.current_stage and stage in ("download", "upload"):
            self.peak_mbps = 0.0
            self.history_mbps.clear()
        self.current_stage = stage
        self.status_detail = message
        self.stage_start_time = time.perf_counter()
        if meta:
            if "client" in meta:
                self.client_info = meta["client"]
            if "servers" in meta:
                self.servers = meta["servers"]

    def update_progress(self, phase: str, samples: list[tuple[float, float]], now: float) -> None:
        mbps = core._rolling_mbps(samples, now)
        if mbps <= 0:
            return
        self.current_mbps = mbps
        # Ignore initial ramp-up seconds to avoid measuring instant connection burst artifacts
        elapsed_stage = now - self.stage_start_time
        if elapsed_stage >= 1.0 and mbps > self.peak_mbps:
            self.peak_mbps = mbps
        self.history_mbps.append(mbps)
        if len(self.history_mbps) > 40:
            self.history_mbps.pop(0)
        self.bytes_transferred = sum(b for _, b in samples)

    def render(self, term_width: int) -> Panel:
        self.spin_index += 1
        spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        spin = spinner_frames[self.spin_index % len(spinner_frames)]

        # Header Info Line
        loc = self.client_info.get("location", {})
        city = loc.get("city", "")
        country = loc.get("country", "")
        loc_str = f"{city}, {country}".strip(", ")
        ip = self.client_info.get("ip", "")
        server_count = len(self.servers) if self.servers else 3

        header_text = Text(no_wrap=True, overflow="ellipsis")
        header_text.append("fast.com", style="bold cyan")
        if term_width >= 60:
            header_text.append("  ·  ", style="bright_black")
            if ip or loc_str:
                header_text.append(f"{loc_str or 'Connected'} ({ip or 'local'})", style="dim")
                header_text.append("  ·  ", style="bright_black")
            header_text.append(f"{server_count} edge targets active", style="dim")
        header_text.append("\n")

        # Top Big Speed Display & Responsive Sparkline
        color = _speed_color(self.current_mbps) if self.current_mbps > 0 else "dim"

        # Dynamically scale sparkline width to prevent line wrapping
        # term_width minus padding (6) minus speed digits & unit (15) minus peak/label (16)
        spark_len = max(6, min(24, term_width - 38))
        if term_width < 50:
            spark_len = 0  # Hide sparkline in very narrow terminals

        sparkline_str = _format_sparkline(self.history_mbps, max(self.peak_mbps, 1.0), max_len=spark_len)

        speed_line = Text(no_wrap=True, overflow="ellipsis")
        if self.current_stage in ("download", "upload"):
            speed_line.append(f"{self.current_mbps:>5.1f}", style=f"bold {color}")
            speed_line.append(" Mbps  ", style=f"{color}")
            if spark_len > 0:
                speed_line.append(f"[{sparkline_str}]", style="cyan")
            if term_width >= 55:
                speed_line.append(f"  peak {self.peak_mbps:g} Mbps", style="dim")
        else:
            speed_line.append("  ---.-", style="dim")
            speed_line.append(" Mbps  ", style="dim")
            if spark_len > 0:
                speed_line.append(f"[{' ' * spark_len}]", style="bright_black")
            if term_width >= 55:
                speed_line.append("  standing by", style="dim")

        # Progress Calculation
        pct = 0.0
        elapsed_stage = time.perf_counter() - self.stage_start_time
        if self.current_stage == "download":
            pct = min(100.0, (elapsed_stage / self.download_duration) * 100.0)
            phase_name = "DOWNLOAD"
        elif self.current_stage == "upload":
            pct = min(100.0, (elapsed_stage / self.upload_duration) * 100.0)
            phase_name = "UPLOAD"
        elif self.current_stage in ("token", "targets", "ping"):
            pct = 100.0 if self.current_stage != "token" else 30.0
            phase_name = "INITIALIZE"
        else:
            pct = 0.0
            phase_name = "WAITING"

        mb_transferred = self.bytes_transferred / (1024 * 1024)

        # Responsive progress bar length
        bar_width = max(8, min(30, term_width - 38))
        bar_visual = _render_progress_bar(pct, width=bar_width)

        progress_line = Text(no_wrap=True, overflow="ellipsis")
        if term_width >= 60:
            dur = self.download_duration if self.current_stage == "download" else self.upload_duration
            timing = f"{elapsed_stage:.1f}s/{dur:g}s" if self.current_stage in ("download", "upload") else ""
            progress_line.append(f"{phase_name:<10} {timing:<9} ", style="dim")
        else:
            progress_line.append(f"{phase_name:<9} ", style="dim")

        if bar_width > 6:
            progress_line.append(bar_visual, style="bright_cyan" if self.current_stage in ("download", "upload") else "dim")
            progress_line.append(" ")
        progress_line.append(f"{pct:>3.0f}%", style="bold bright_white")
        if mb_transferred > 0 and term_width >= 70:
            progress_line.append(f"  ({mb_transferred:.1f} MB)", style="dim")

        # Pipeline Steps
        pipeline_table = Table.grid(expand=True, padding=(0, 1))
        pipeline_table.add_column(width=2)
        pipeline_table.add_column(overflow="ellipsis")

        stage_order = [s[0] for s in PIPELINE_STEPS]
        cur_idx = stage_order.index(self.current_stage) if self.current_stage in stage_order else 2

        for idx, (st_id, st_name, st_desc) in enumerate(PIPELINE_STEPS):
            if st_id == "upload" and self.skip_upload:
                pipeline_table.add_row(
                    Text("○", style="dim bright_black"),
                    Text(f"{st_name} (skipped)", style="dim bright_black"),
                )
            elif idx < cur_idx or (self.current_stage == "upload_skip" and idx <= 4):
                pipeline_table.add_row(
                    Text("✓", style="green bold"),
                    Text(st_name, style="bright_white"),
                )
            elif idx == cur_idx:
                pipeline_table.add_row(
                    Text(spin, style="bold cyan"),
                    Text(st_name, style="bold cyan"),
                )
            else:
                pipeline_table.add_row(
                    Text("○", style="dim bright_black"),
                    Text(st_name, style="dim bright_black"),
                )

        # Telemetry info
        telemetry_table = Table.grid(expand=True, padding=(0, 1))
        telemetry_table.add_column(style="dim", width=12)
        telemetry_table.add_column(style="bright_white", overflow="ellipsis")

        telemetry_table.add_row("Current State", self.status_detail)
        telemetry_table.add_row("Active Streams", f"{server_count} HTTP/2 workers")
        telemetry_table.add_row(
            "Transfer Total",
            f"{mb_transferred:.1f} MB payload" if mb_transferred > 0 else "0.0 MB",
        )

        pipeline_box = Panel(
            pipeline_table,
            title=Text(" pipeline ", style="dim"),
            border_style="bright_black",
            box=box.ROUNDED,
            padding=(0, 1),
        )
        telemetry_box = Panel(
            telemetry_table,
            title=Text(" telemetry ", style="dim"),
            border_style="bright_black",
            box=box.ROUNDED,
            padding=(0, 1),
        )

        # Responsive Layout: Stack columns vertically if terminal is narrow (< 75 cols)
        if term_width < 75:
            bottom_section = Group(pipeline_box, telemetry_box)
        else:
            columns_table = Table.grid(expand=True, padding=(0, 1))
            columns_table.add_column(ratio=1)
            columns_table.add_column(ratio=1)
            columns_table.add_row(pipeline_box, telemetry_box)
            bottom_section = columns_table

        main_group = Group(
            header_text,
            speed_line,
            Text(""),
            progress_line,
            Text(""),
            bottom_section,
        )

        return Panel(
            main_group,
            border_style="bright_black",
            box=box.ROUNDED,
            padding=(0, 1) if term_width < 60 else (1, 2),
        )


@app.command()
def main(
    json_output: Annotated[bool, typer.Option("--json", help="output as JSON")] = False,
    no_upload: Annotated[bool, typer.Option("--no-upload", help="skip the upload test")] = False,
    url_count: Annotated[int, typer.Option("--url-count", min=1, help="number of servers to use")] = 3,
    download_duration: Annotated[
        float, typer.Option("--download-duration", min=1, help="seconds for the download test")
    ] = 10.0,
    upload_duration: Annotated[
        float, typer.Option("--upload-duration", min=1, help="seconds for the upload test")
    ] = 8.0,
) -> None:
    """Internet speed test powered by fast.com with a live responsive UI."""

    if json_output:
        try:
            result = asyncio.run(
                core.run_test(
                    url_count=url_count,
                    download_duration=download_duration,
                    upload_duration=upload_duration,
                    skip_upload=no_upload,
                )
            )
        except SpeedTestError as e:
            print(json.dumps({"error": str(e)}), file=sys.stderr)
            raise typer.Exit(1)
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        return

    ui_state = HarnessUIState(
        download_duration=download_duration,
        upload_duration=upload_duration,
        skip_upload=no_upload,
    )

    def on_status(stage: str, msg: str, meta: dict | None = None) -> None:
        ui_state.set_stage(stage, msg, meta)

    def on_progress(phase: str, samples: list[tuple[float, float]], now: float) -> None:
        ui_state.update_progress(phase, samples, now)

    t0 = time.perf_counter()
    result = None

    try:
        with Live(
            ui_state.render(console.width),
            console=console,
            refresh_per_second=24,
            transient=True,
            auto_refresh=False,
        ) as live:
            async def orchestrate() -> SpeedTestResult:
                async def refresh_loop():
                    while True:
                        # Dynamically adapt to current console width on every frame
                        current_width = console.width
                        live.update(ui_state.render(current_width), refresh=True)
                        await asyncio.sleep(0.04)

                refresher = asyncio.create_task(refresh_loop())
                try:
                    return await core.run_test(
                        url_count=url_count,
                        download_duration=download_duration,
                        upload_duration=upload_duration,
                        skip_upload=no_upload,
                        on_status=on_status,
                        on_progress=on_progress,
                    )
                finally:
                    refresher.cancel()

            result = asyncio.run(orchestrate())
    except SpeedTestError as e:
        console.print(f"[red bold]Speed Test Error:[/] {e}")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print("\n[dim yellow]Test aborted by user[/]")
        raise typer.Exit(130)

    elapsed = time.perf_counter() - t0
    console.print(_render_summary(result, elapsed, console.width))


if __name__ == "__main__":
    app()
