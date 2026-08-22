from typing import List, Optional

from textual.app import App
from textual.containers import Horizontal, Vertical
from textual.widgets import Label, ListItem, ListView, Static

from ydb.tools.mnc.cli import arg_metadata


class _CommandListItem(ListItem):
    def __init__(self, command: arg_metadata.CommandMeta):
        super().__init__(Label(command.name))
        self.command = command


class _BackListItem(ListItem):
    def __init__(self):
        super().__init__(Label(".. back"))


class CommandPickerApp(App[arg_metadata.CommandMeta]):
    CSS = """
    Screen { layout: vertical; background: transparent; }
    #title { height: 3; padding: 1 2; text-style: bold; color: $accent; }
    #body { layout: horizontal; height: 1fr; padding: 0 1 1 1; }
    #left { width: 2fr; height: 1fr; border: solid $primary; }
    #details { width: 3fr; height: 1fr; padding: 1 2; border: solid $primary; margin-left: 1; }
    #commands { height: 1fr; background: transparent; }
    ListView { background: transparent; }
    ListItem { background: transparent; }
    ListItem.--highlight { background: $primary; text-style: bold; }
    """
    BINDINGS = [("escape", "back_or_cancel", "Back")]

    def __init__(self, root: arg_metadata.CommandMeta, initial: Optional[arg_metadata.CommandMeta] = None):
        super().__init__()
        self.root = root
        self.command_parent = initial or root
        self.stack = command_stack(root, self.command_parent)

    def compose(self):
        yield Static("Select command", id="title")
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield ListView(id="commands")
            yield Static("", id="details")

    def on_mount(self):
        self._refresh()
        self.query_one("#commands", ListView).focus()

    def action_back_or_cancel(self):
        if self._pop_command_level():
            self._refresh()
            return
        self.exit(None)

    def on_list_view_highlighted(self, event: ListView.Highlighted):
        event.stop()
        if isinstance(event.item, _CommandListItem):
            self._show_details(event.item.command)
        elif isinstance(event.item, _BackListItem) and len(self.stack) > 1:
            self._show_details(self.stack[-2])

    def on_list_view_selected(self, event: ListView.Selected):
        event.stop()
        if isinstance(event.item, _CommandListItem):
            command = event.item.command
            if command.children:
                self.stack.append(command)
                self.command_parent = command
                self._refresh()
            else:
                self.exit(command)
        elif isinstance(event.item, _BackListItem):
            if self._pop_command_level():
                self._refresh()

    def _pop_command_level(self) -> bool:
        if len(self.stack) <= 1:
            return False
        self.stack.pop()
        self.command_parent = self.stack[-1]
        return True

    def _refresh(self):
        list_view = self.query_one("#commands", ListView)
        list_view.clear()
        items = []
        if len(self.stack) > 1:
            items.append(_BackListItem())
        items.extend(_CommandListItem(command) for command in self.command_parent.children)
        list_view.extend(items)
        list_view.index = 0 if items else None
        self.query_one("#title", Static).update("Select command: " + command_path_text(self.command_parent))
        if items:
            first = items[0]
            if isinstance(first, _CommandListItem):
                self._show_details(first.command)
            else:
                self._show_details(self.stack[-2])
        else:
            self.query_one("#details", Static).update("No commands")

    def _show_details(self, command: arg_metadata.CommandMeta):
        self.query_one("#details", Static).update(arg_metadata.command_help_text(command))


def command_stack(root: arg_metadata.CommandMeta, command: arg_metadata.CommandMeta) -> List[arg_metadata.CommandMeta]:
    if command is root:
        return [root]
    stack = [root]
    for part in command.path:
        current = stack[-1]
        for child in current.children:
            if child.name == part:
                stack.append(child)
                break
        else:
            return [root]
    return stack


def command_path_text(command: arg_metadata.CommandMeta) -> str:
    return " ".join(command.path) if command.path else "mnc"
