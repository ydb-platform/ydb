#! /usr/bin/python3

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

import bridge


logger = logging.getLogger(__name__)

# TODO: fast and dirty way
def _colorize_log_line(line: str) -> str:
    # Expected format: "%(asctime)s %(levelname)s %(name)s: %(message)s"
    # We detect the level token inside the line and colorize for CRITICAL/ERROR/WARNING
    try:
        # Find level token position by scanning for known tokens with surrounding spaces
        candidates = [
            (" CRITICAL ", "red"),
            (" ERROR ", "#ff0000"), # for some reason "red" in markup is magenta like
            (" WARNING ", "#ffff00"), # "yellow" is red :/
        ]
        hit = None
        hit_idx = None
        for token, color in candidates:
            idx = line.find(token)
            if idx != -1 and (hit_idx is None or idx < hit_idx):
                hit = (token.strip(), color)
                hit_idx = idx
        if hit is None:
            return escape(line)

        level_text, color = hit
        token_len = len(level_text) + 2  # include surrounding spaces
        prefix = line[:hit_idx]
        after = line[hit_idx + token_len :]
        return f"{escape(prefix)} [{color}]{level_text}[/] {escape(after)}"
    except Exception:
        return escape(line)


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
    #history_view { height: 10; }
    #logs_view { height: 1fr; }
    .pile { width: 25; margin: 0 1; align: center middle; }
    """

    def __init__(
        self,
        keeper: bridge.Bridgekeeper,
        cluster_name: str,
        refresh_seconds: float,
        auto_failover: bool,
        log_consumer: Callable[[], List[str]],
    ):
        super().__init__()

        self.keeper = keeper
        self.cluster_name = cluster_name
        self.refresh_seconds = refresh_seconds
        self.auto_failover = auto_failover
        self.log_consumer = log_consumer

        self._refresh_timer = None

        self.pile_widgets: Dict[str, PileWidget] = {}

        self.keeper.run_async()

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
        self.history_view.auto_scroll = True
        yield self.history_view

        # Logs section
        yield Static("Logs", classes="section_title")
        self.log_view = RichLog(id="logs_view")
        self.log_view.auto_scroll = True
        self.log_view.markup = True
        yield self.log_view

    async def on_mount(self) -> None:
        # Single managed timer
        self._refresh_timer = self.set_interval(self.refresh_seconds, self.refresh_once, pause=False)

    # TODO
    async def on_key(self, event: events.Key) -> None:
        pass

    async def refresh_once(self) -> None:
        state, transitions = self.keeper.get_state_and_history()

        now = dt.datetime.utcnow()

        if self.header:
            self.header.now_utc = now

        # Compute statuses and colors
        current_status_color: Dict[str, [str, str]] = {}
        if state:
            for pile_name, pile_state in state.Piles.items():
                current_status_color[pile_name] = [pile_state.get_state(), pile_state.get_color(),]

            # Ensure widgets exist for all piles
            # TODO: remove old piles?
            for pile_name in current_status_color.keys():
                if pile_name not in self.pile_widgets:
                    w = PileWidget(classes="pile")
                    w.pile_name = pile_name
                    self.pile_widgets[pile_name] = w

            # Stable order by name; clear and mount all widgets
            ordered_names = sorted(current_status_color.keys())
            self.piles_group.remove_children()
            widgets_to_mount = []
            for name in ordered_names:
                widget = self.pile_widgets[name]
                widget.status = current_status_color[name][0]
                widget.color = current_status_color[name][1]
                widgets_to_mount.append(widget)
            if widgets_to_mount:
                self.piles_group.mount(*widgets_to_mount)

        # TODO: show disaster and disaster stopwatch

        # Update transitions from history

        if transitions:
            # TODO: append only new ones
            self.history_view.clear()
            self.history_view.write_lines(list(transitions[-50:]))

        # Update logs

        new_lines = self.log_consumer()
        if len(new_lines) > 0:
            new_lines = [_colorize_log_line(l) for l in new_lines]
            joined_lines = '\n'.join(new_lines)
            self.log_view.write(joined_lines)
