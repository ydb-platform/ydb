#! /usr/bin/python3

import bridge

import datetime as dt
import logging
import os
import requests
import shutil
import sys

from typing import Dict, List, Optional, Tuple, Callable

from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.widgets import Static, Log, RichLog

from rich.markup import escape
from rich.text import Text


logger = logging.getLogger(__name__)


class MarkupFormatter:
    def __init__(self, fmt: str):
        self._fmt = fmt

    def format(self, record: logging.LogRecord) -> str:
        try:
            message = self._fmt % {
                "asctime": self._format_time(record),
                "levelname": record.levelname,
                "name": record.name,
                "message": record.getMessage(),
            }
        except Exception:
            message = record.getMessage()

        # Colorize based on level
        try:
            # Escape message except the level token we will colorize separately
            level_color = {
                "CRITICAL": "red",
                "ERROR": "#ff0000",
                "WARNING": "#ffff00",
            }.get(record.levelname)
            if level_color is None:
                return escape(message)
            # Replace first occurrence of level token with colored version
            token = f" {record.levelname} "
            idx = message.find(token)
            if idx == -1:
                return escape(message)
            prefix = message[:idx]
            after = message[idx + len(token):]
            return f"{escape(prefix)} [{level_color}]{record.levelname}[/] {escape(after)}"
        except Exception:
            return escape(message)

    def _format_time(self, record: logging.LogRecord) -> str:
        import datetime as _dt
        return _dt.datetime.utcfromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")


class HeaderBar(Static):
    cluster_name: reactive[str] = reactive("")
    refresh_seconds: reactive[float] = reactive(1.0)
    auto_failover: reactive[bool] = reactive(False)
    now_utc: reactive[dt.datetime] = reactive(dt.datetime.utcnow())

    def render(self) -> str:
        now_str = self.now_utc.strftime("%Y-%m-%d %H:%M:%S (UTC)")
        auto = "ON" if self.auto_failover else "OFF"
        return (
            f"Cluster: {self.cluster_name}    Now: {now_str}    Refresh: {self.refresh_seconds:.1f}s    Auto-Failover: {auto}\n"
            f"Legend: [green]PRIMARY/SYNCHRONIZED[/]  [red]DISCONNECTED[/]  [yellow]NOT_SYNCHRONIZED[/]  "
            f"[cyan]PROMOTED[/]  [magenta]DEMOTED[/]  [grey50]SUSPENDED[/]"
        )


class PileWidget(Static):
    pile_name: reactive[str] = reactive("")
    status: reactive[str] = reactive("DISCONNECTED")
    color: reactive[str] = reactive("red")

    def render(self) -> str:
        inner_w = bridge.get_max_status_length() + 2

        lines = [
            (f"[{self.color}]" +
            f"╭{'─' * inner_w}╮"),
            f"│{self.pile_name.center(inner_w)}│",
            f"│{self.status.center(inner_w)}│",
            f"╰{'─' * inner_w}╯ [/]",
        ]
        return "\n".join(lines)


class KeeperApp(App):
    CSS = """
    Screen { layout: vertical; }
    #header { height: 5; }
    .section_title { content-align: center middle; height: 1; }
    #piles_group { height: 10; align: center middle; content-align: center middle; }
    #history_view { height: 20; }
    #logs_view { height: 1fr; }
    .pile { width: 25; height: 5; margin: 0 1; align: center middle; content-align: center middle; }
    """

    def __init__(
        self,
        keeper: bridge.BridgeSkipper,
        cluster_name: str,
        refresh_seconds: float,
        auto_failover: bool,
        log_consumer: Callable[[], List[logging.LogRecord]],
    ):
        super().__init__()

        self.keeper = keeper
        self.cluster_name = cluster_name
        self.refresh_seconds = refresh_seconds
        self.auto_failover = auto_failover
        self.log_consumer = log_consumer

        self._refresh_timer = None

        self.pile_widgets: Dict[str, PileWidget] = {}
        self.prev_transitions = None

        self._keeper_thread = self.keeper.run_async()
        if not self._keeper_thread:
            raise("Failed to start async keeper")

    def compose(self) -> ComposeResult:
        self.header = HeaderBar(id="header")
        self.header.cluster_name = self.cluster_name
        self.header.refresh_seconds = self.refresh_seconds
        self.header.auto_failover = self.auto_failover
        yield self.header

        yield Static("Piles", classes="section_title")
        self.piles_group = Horizontal(id="piles_group")
        yield self.piles_group

        yield Static("Transition History", classes="section_title")
        self.history_view = Log(id="history_view")
        self.history_view.auto_scroll = False
        yield self.history_view

        # Logs section
        yield Static("Logs", classes="section_title")
        self.log_view = RichLog(id="logs_view")
        self.log_view.auto_scroll = False
        self.log_view.markup = True
        self.log_view.wrap = True
        yield self.log_view

    async def on_mount(self) -> None:
        # Single managed timer
        self._refresh_timer = self.set_interval(self.refresh_seconds, self.refresh_once, pause=False)

    # TODO
    async def on_key(self, event: events.Key) -> None:
        pass

    async def refresh_once(self) -> None:
        if not self._keeper_thread.is_alive():
            self.exit(1)

        state, transitions = self.keeper.get_state_and_history()

        now = dt.datetime.utcnow()

        if self.header:
            self.header.now_utc = now

        # Compute statuses and colors
        current_status_color: Dict[str, [str, str]] = {}
        if state:
            for pile_name, pile_state in state.piles.items():
                current_status_color[pile_name] = [pile_state.get_state(), pile_state.get_color(),]

            # Ensure widgets exist for all piles
            widgets_to_mount = []
            ordered_names = sorted(current_status_color.keys())
            for pile_name in ordered_names:
                if pile_name not in self.pile_widgets:
                    w = PileWidget(classes="pile")
                    w.pile_name = pile_name
                    self.pile_widgets[pile_name] = w
                    widgets_to_mount.append(w)

            if widgets_to_mount:
                await self.piles_group.mount(*widgets_to_mount)
                self.piles_group.refresh(layout=True)

            for name in ordered_names:
                widget = self.pile_widgets[name]
                widget.status = current_status_color[name][0]
                widget.color = current_status_color[name][1]

        # Update transitions from history

        if transitions and transitions != self.prev_transitions:
            # TODO: append only new ones
            self.history_view.clear()
            self.history_view.write_lines(list(transitions[-50:]))

            self.prev_transitions = transitions

        # Update logs
        new_records = self.log_consumer()
        if len(new_records) > 0:
            formatter = MarkupFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
            lines = [formatter.format(r) for r in new_records]
            joined_lines = "\n".join(lines)
            self.log_view.write(joined_lines)
