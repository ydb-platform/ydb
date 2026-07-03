from typing import List

from textual.app import App
from textual.containers import Horizontal, Vertical
from textual.widgets import Label, ListItem, ListView, Static

from ydb.tools.mnc.cli.tui.common import ConfigCandidate, config_preview


class _ConfigListItem(ListItem):
    def __init__(self, candidate: ConfigCandidate):
        super().__init__(Label(f"{candidate.name}  {candidate.path}"))
        self.candidate = candidate


class ConfigPickerApp(App[tuple[str, str]]):
    CSS = """
    Screen { layout: vertical; background: transparent; }
    #title { height: 3; padding: 1 2; text-style: bold; color: $accent; }
    #body { layout: horizontal; height: 1fr; padding: 0 1 1 1; }
    #left { width: 2fr; height: 1fr; border: solid $primary; }
    #details { width: 3fr; height: 1fr; padding: 1 2; border: solid $primary; margin-left: 1; }
    #configs { height: 1fr; background: transparent; }
    ListView { background: transparent; }
    ListItem { background: transparent; }
    ListItem.--highlight { background: $primary; text-style: bold; }
    """
    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, candidates: List[ConfigCandidate], command_scheme=None):
        super().__init__()
        self.candidates = candidates
        self.command_scheme = command_scheme

    def compose(self):
        yield Static("Select config", id="title")
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield ListView(id="configs")
            yield Static("", id="details")

    def on_mount(self):
        self._refresh()
        self.query_one("#configs", ListView).focus()

    def action_cancel(self):
        self.exit(None)

    def on_list_view_highlighted(self, event: ListView.Highlighted):
        event.stop()
        if isinstance(event.item, _ConfigListItem):
            self._show_details(event.item.candidate)

    def on_list_view_selected(self, event: ListView.Selected):
        event.stop()
        if isinstance(event.item, _ConfigListItem):
            self.exit(("--config", event.item.candidate.name))

    def _refresh(self):
        list_view = self.query_one("#configs", ListView)
        list_view.clear()
        list_view.extend([_ConfigListItem(candidate) for candidate in self.candidates])
        list_view.index = 0 if self.candidates else None
        if self.candidates:
            self._show_details(self.candidates[0])
        else:
            self.query_one("#details", Static).update("No configs found")

    def _show_details(self, candidate: ConfigCandidate):
        self.query_one("#details", Static).update(config_preview(candidate, self.command_scheme))
