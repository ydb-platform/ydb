from textual.app import App
from textual.containers import Container, ScrollableContainer
from textual.widgets import Button, Checkbox, Input, Label, Static

from ydb.tools.mnc.cli.tui.common import field_id, option_label, value_to_text
import ydb.tools.mnc.scheme as scheme


DEPLOY_FLAGS_DEST = "deploy_flags"


class OptionsFormApp(App[dict]):
    CSS = """
    Screen { layout: vertical; background: transparent; }
    #title { height: 3; padding: 1 2; text-style: bold; color: $accent; }
    #form { height: 1fr; padding: 1 2; margin: 0 1; border: solid $primary; background: transparent; }
    .field { height: auto; margin-bottom: 1; }
    .hint { color: $text-muted; }
    #buttons { height: 3; padding: 0 1; align-horizontal: right; }
    Button { margin-left: 1; }
    Input { background: transparent; border: solid $primary; }
    Checkbox { background: transparent; }
    """
    BINDINGS = [("escape", "cancel", "Cancel"), ("ctrl+s", "submit", "Apply")]

    def __init__(self, title: str, initial_args, options=None, arguments=None):
        super().__init__()
        self.title = title
        self.initial_args = initial_args
        self.deploy_flags_options = [
            option for option in (options or [])
            if option.dest == DEPLOY_FLAGS_DEST
        ]
        self.value_options = [
            option for option in (options or [])
            if option.expects_value and option.dest != DEPLOY_FLAGS_DEST
        ]
        self.flag_options = [
            option for option in (options or [])
            if not option.expects_value
        ]
        self.arguments = list(arguments or [])

    def compose(self):
        yield Static(self.title, id="title")
        with ScrollableContainer(id="form"):
            for option in self.flag_options:
                value = bool(getattr(self.initial_args, option.dest, option.default))
                label = option_label(option)
                yield Checkbox(label, value=value, id=field_id("flag", option.dest), classes="field")
            for option in self.value_options:
                current = getattr(self.initial_args, option.dest, option.default)
                yield Label(option_label(option), classes="field")
                if option.help:
                    yield Static(option.help, classes="hint field")
                if option.choices:
                    yield Static("Choices: " + ", ".join(map(str, option.choices)), classes="hint field")
                yield Input(value_to_text(current), placeholder=option.metavar or option.dest or "", id=field_id("opt", option.dest), classes="field")
            for option in self.deploy_flags_options:
                yield Label(option_label(option), classes="field")
                if option.help:
                    yield Static(option.help, classes="hint field")
                selected_flags = set(getattr(self.initial_args, option.dest, option.default) or [])
                for deploy_flag in scheme.common.deploy_flags:
                    yield Checkbox(
                        deploy_flag,
                        value=deploy_flag in selected_flags,
                        id=_deploy_flag_field_id(deploy_flag),
                        classes="field",
                    )
            for argument in self.arguments:
                current = getattr(self.initial_args, argument.dest, None)
                yield Label(argument.name, classes="field")
                if argument.help:
                    yield Static(argument.help, classes="hint field")
                if argument.choices:
                    yield Static("Choices: " + ", ".join(map(str, argument.choices)), classes="hint field")
                yield Input(value_to_text(current), placeholder=argument.metavar or argument.name, id=field_id("arg", argument.dest), classes="field")
        with Container(id="buttons"):
            yield Button("Apply", id="apply", variant="primary")

    def action_cancel(self):
        self.exit(None)

    def action_submit(self):
        self._submit()

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "apply":
            self._submit()

    def on_checkbox_changed(self, event: Checkbox.Changed):
        if not event.value or event.checkbox.id is None:
            return
        deploy_flag = _deploy_flag_from_field_id(event.checkbox.id)
        if deploy_flag is None:
            return
        opposite_flag = scheme.common.opposite_deploy_flags.get(deploy_flag)
        if opposite_flag is None:
            return
        opposite_checkbox = self.query_one("#" + _deploy_flag_field_id(opposite_flag), Checkbox)
        opposite_checkbox.value = False

    def _submit(self):
        result = {
            "flags": {},
            "options": {},
            "arguments": {},
        }
        for option in self.flag_options:
            widget = self.query_one("#" + field_id("flag", option.dest), Checkbox)
            result["flags"][option.dest] = bool(widget.value)
        for option in self.value_options:
            widget = self.query_one("#" + field_id("opt", option.dest), Input)
            result["options"][option.dest] = widget.value
        for option in self.deploy_flags_options:
            selected_flags = []
            for deploy_flag in scheme.common.deploy_flags:
                widget = self.query_one("#" + _deploy_flag_field_id(deploy_flag), Checkbox)
                if widget.value:
                    selected_flags.append(deploy_flag)
            result["options"][option.dest] = selected_flags
        for argument in self.arguments:
            widget = self.query_one("#" + field_id("arg", argument.dest), Input)
            result["arguments"][argument.dest] = widget.value
        self.exit(result)


def _deploy_flag_field_id(deploy_flag: str) -> str:
    return field_id("deploy-flag", deploy_flag)


def _deploy_flag_from_field_id(widget_id: str):
    prefix = "deploy-flag-"
    if not widget_id.startswith(prefix):
        return None
    return widget_id[len(prefix):].replace("-", "_")
