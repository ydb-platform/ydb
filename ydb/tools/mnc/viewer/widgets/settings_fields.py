from dataclasses import dataclass
from typing import Optional

from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import Key
from textual.widgets import Checkbox, Input, Label, ListItem, ListView


CONFIG_FIELD_TITLES = {
    "git_ydb_root": "YDB source root",
}
MNC_DEPLOY_FLAG_CHECKBOX_PREFIX = "mnc-deploy-flag-"


@dataclass(frozen=True)
class MncDeployFlagOption:
    option_id: str
    title: str
    enabled_flag: str
    disabled_flag: Optional[str] = None
    default_enabled: bool = False


MNC_DEPLOY_FLAG_OPTIONS = (
    MncDeployFlagOption("rebuild-binary", "Rebuild binary", "do_rebuild", "do_not_rebuild", True),
    MncDeployFlagOption("strip-binary", "Strip binary", "do_strip", "do_not_strip", True),
    MncDeployFlagOption("redeploy-binary", "Redeploy binary", "do_redeploy_bin", "do_not_redeploy_bin", True),
    MncDeployFlagOption("transit-binary", "Transit binary through first node", "transit_bin_through_first_node"),
    MncDeployFlagOption("secure-mode", "Secure mode", "secure"),
)
MNC_DEPLOY_FLAG_OPTION_ROWS = (
    MNC_DEPLOY_FLAG_OPTIONS[:2],
    MNC_DEPLOY_FLAG_OPTIONS[2:4],
    MNC_DEPLOY_FLAG_OPTIONS[4:],
)


def mnc_deploy_flag_checkbox_id(option_id: str) -> str:
    return MNC_DEPLOY_FLAG_CHECKBOX_PREFIX + option_id


def mnc_deploy_flag_option_from_checkbox_id(checkbox_id: Optional[str]) -> Optional[MncDeployFlagOption]:
    if checkbox_id is None or not checkbox_id.startswith(MNC_DEPLOY_FLAG_CHECKBOX_PREFIX):
        return None
    option_id = checkbox_id[len(MNC_DEPLOY_FLAG_CHECKBOX_PREFIX):]
    return next((option for option in MNC_DEPLOY_FLAG_OPTIONS if option.option_id == option_id), None)


def mnc_deploy_flag_option_value(option: MncDeployFlagOption, deploy_flags: set[str]) -> bool:
    if option.enabled_flag in deploy_flags:
        return True
    if option.disabled_flag is not None and option.disabled_flag in deploy_flags:
        return False
    return option.default_enabled


class ConfigFieldItem(ListItem):
    def __init__(self, field_name: str, title: str, value: str, path_picker: bool = False) -> None:
        self.field_name = field_name
        self.title = title
        self.path_picker = path_picker
        self._saved_value = value
        self.input = Input(value=value, id=f"config-field-{field_name}")
        self.input.disabled = True
        super().__init__(
            Horizontal(
                Label(title, classes="config-field-title"),
                Vertical(self.input, classes="config-field-value"),
                classes="config-field-row",
            )
        )

    @property
    def value(self) -> str:
        return self.input.value

    def start_edit(self) -> None:
        self._saved_value = self.input.value
        self.input.disabled = False
        self.input.focus()
        self.input.cursor_position = len(self.input.value)

    def save_edit(self) -> None:
        self._saved_value = self.input.value
        self.input.disabled = True

    def cancel_edit(self) -> None:
        self.input.value = self._saved_value
        self.input.disabled = True


class MncConfigFieldsList(ListView, can_focus=True, can_focus_children=False, inherit_bindings=False):
    BINDINGS = [
        Binding("enter", "select_cursor", "Select", show=False),
    ]

    def key_down(self, event: Key) -> None:
        form = self.parent
        if hasattr(form, "focus_deploy_flag"):
            form.focus_deploy_flag(0, 0)
            event.stop()


class MncDeployFlagCheckbox(Checkbox):
    def _form(self):
        parent = self.parent
        while parent is not None:
            if hasattr(parent, "move_deploy_flag_from"):
                return parent
            parent = parent.parent
        return None

    def _move(self, event: Key, row_delta: int = 0, col_delta: int = 0) -> None:
        form = self._form()
        if form is not None and form.move_deploy_flag_from(self.id, row_delta, col_delta):
            event.stop()

    def key_up(self, event: Key) -> None:
        self._move(event, row_delta=-1)

    def key_down(self, event: Key) -> None:
        self._move(event, row_delta=1)

    def key_left(self, event: Key) -> None:
        self._move(event, col_delta=-1)

    def key_right(self, event: Key) -> None:
        self._move(event, col_delta=1)
