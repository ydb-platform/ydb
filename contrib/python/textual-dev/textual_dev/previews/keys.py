from __future__ import annotations

from typing import Optional

from rich.panel import Panel
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.reactive import var
from textual.widgets import Button, Header, RichLog

INSTRUCTIONS = """\
[u]Press some keys![/]

To quit the app press [b]ctrl+c[/b] [i]twice[/i] or press the Quit button below.\
"""


class KeyLog(RichLog, inherit_bindings=False):
    """We don't want to handle scroll keys."""


class KeysApp(App[None], inherit_bindings=False):
    """Show key events in a text log."""

    TITLE = "Textual Keys"
    BINDINGS = [("c", "clear", "Clear")]
    CSS = """
    #buttons {
        dock: bottom;
        height: 3;
    }
    Button {
        width: 1fr;
    }
    """
    ENABLE_COMMAND_PALETTE = False

    last_key: var[str | None] = var[Optional[str]](None)

    def compose(self) -> ComposeResult:
        yield Header()
        yield KeyLog()
        yield Horizontal(
            Button("Clear", id="clear", variant="warning"),
            Button("Quit", id="quit", variant="error"),
            id="buttons",
        )

    def on_ready(self) -> None:
        self.query_one(KeyLog).write(Panel(Text.from_markup(INSTRUCTIONS)), expand=True)

    def on_key(self, event: events.Key) -> None:
        self.query_one(KeyLog).write(event)
        if event.key == "ctrl+c":
            if self.last_key == "ctrl+c":
                self.exit()
            else:
                self.query_one(KeyLog).write("Press Ctrl+C again to quit")

        self.last_key = event.key

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.exit()
        elif event.button.id == "clear":
            self.query_one(KeyLog).clear()


if __name__ == "__main__":
    KeysApp().run()
