from typing import Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static


class InvalidPathModal(ModalScreen[None]):
    CSS = """
    InvalidPathModal {
        color: $foreground;
        background: $background 60%;
        align: center middle;
    }

    #invalid-path-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        border: hkey $error;
        background: $surface;
    }

    #invalid-path-dialog:dark {
        background: $panel-darken-1;
    }

    #invalid-path-title {
        height: auto;
        text-style: bold;
        color: $error;
        margin-bottom: 1;
    }

    #invalid-path-message {
        height: auto;
        margin-bottom: 1;
    }

    #invalid-path-actions {
        height: auto;
        align-horizontal: right;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
    ]

    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = path

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Invalid path", id="invalid-path-title"),
            Static(
                f"Path is not suitable for YDB source root:\n{self._path}\n\nSelect an existing directory.",
                id="invalid-path-message",
            ),
            Horizontal(
                Button("OK", id="invalid-path-ok", variant="primary"),
                id="invalid-path-actions",
            ),
            id="invalid-path-dialog",
        )

    def on_mount(self) -> None:
        self.query_one("#invalid-path-ok", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "invalid-path-ok":
            event.stop()
            self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


class MessageModal(ModalScreen[None]):
    CSS = """
    MessageModal {
        color: $foreground;
        background: $background 60%;
        align: center middle;
    }

    #message-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        border: hkey $error;
        background: $surface;
    }

    #message-dialog:dark {
        background: $panel-darken-1;
    }

    #message-title {
        height: auto;
        text-style: bold;
        color: $error;
        margin-bottom: 1;
    }

    #message-body {
        height: auto;
        margin-bottom: 1;
    }

    #message-actions {
        height: auto;
        align-horizontal: right;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
    ]

    def __init__(self, title: str, message: str) -> None:
        super().__init__()
        self._title = title
        self._message = message

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(self._title, id="message-title"),
            Static(self._message, id="message-body"),
            Horizontal(
                Button("OK", id="message-ok", variant="primary"),
                id="message-actions",
            ),
            id="message-dialog",
        )

    def on_mount(self) -> None:
        self.query_one("#message-ok", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "message-ok":
            event.stop()
            self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


class ConfigNameModal(ModalScreen[Optional[str]]):
    CSS = """
    ConfigNameModal {
        color: $foreground;
        background: $background 60%;
        align: center middle;
    }

    #config-name-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        border: hkey $border;
        background: $surface;
    }

    #config-name-dialog:dark {
        background: $panel-darken-1;
    }

    #config-name-title {
        height: auto;
        text-style: bold;
        margin-bottom: 1;
    }

    #config-name-input {
        height: auto;
        margin-bottom: 1;
    }

    #config-name-actions {
        height: auto;
        align-horizontal: right;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(self, title: str, initial_name: str, action_label: str) -> None:
        super().__init__()
        self._title = title
        self._initial_name = initial_name
        self._action_label = action_label

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(self._title, id="config-name-title"),
            Input(value=self._initial_name, id="config-name-input"),
            Horizontal(
                Button(self._action_label, id="config-name-ok", variant="primary"),
                Button("Cancel", id="config-name-cancel"),
                id="config-name-actions",
            ),
            id="config-name-dialog",
        )

    def on_mount(self) -> None:
        name_input = self.query_one("#config-name-input", Input)
        name_input.focus()
        name_input.cursor_position = len(name_input.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "config-name-input":
            event.stop()
            self._accept()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "config-name-ok":
            event.stop()
            self._accept()
        elif event.button.id == "config-name-cancel":
            event.stop()
            self.dismiss(None)

    def _accept(self) -> None:
        self.dismiss(self.query_one("#config-name-input", Input).value.strip())

    def action_cancel(self) -> None:
        self.dismiss(None)
