from typing import Optional

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Checkbox, Label, ListView

from ydb.tools.mnc.viewer.styles.settings import SETTINGS_CSS
from ydb.tools.mnc.viewer.widgets.settings_fields import (
    CONFIG_FIELD_TITLES,
    MNC_DEPLOY_FLAG_OPTION_ROWS,
    ConfigFieldItem,
    MncConfigFieldsList,
    MncDeployFlagCheckbox,
    mnc_deploy_flag_checkbox_id,
    mnc_deploy_flag_option_from_checkbox_id,
    mnc_deploy_flag_option_value,
)


class MncConfigForm(Vertical):
    DEFAULT_CSS = SETTINGS_CSS

    def __init__(self, config: dict[str, object], default_git_ydb_root: str) -> None:
        super().__init__()
        self._config = config
        self._default_git_ydb_root = default_git_ydb_root

    def compose(self) -> ComposeResult:
        yield MncConfigFieldsList(
            ConfigFieldItem(
                "git_ydb_root",
                CONFIG_FIELD_TITLES["git_ydb_root"],
                self._config.get("git_ydb_root", self._default_git_ydb_root),
                path_picker=True,
            ),
            initial_index=0,
            id="mnc-config-fields",
        )
        deploy_flags = set(self._config.get("deploy_flags") or [])
        yield Vertical(
            Label("Deploy flags", classes="config-group-title"),
            *(
                Horizontal(
                    *(
                        MncDeployFlagCheckbox(
                            option.title,
                            value=mnc_deploy_flag_option_value(option, deploy_flags),
                            id=mnc_deploy_flag_checkbox_id(option.option_id),
                            classes="config-checkbox",
                        )
                        for option in row
                    ),
                    classes="config-checkbox-row",
                )
                for row in MNC_DEPLOY_FLAG_OPTION_ROWS
            ),
            id="mnc-deploy-flags",
        )

    def focus_config_fields(self) -> None:
        fields = self.query_one("#mnc-config-fields", ListView)
        fields.index = 0
        self.screen.set_focus(fields)

    def focus_deploy_flag(self, row: int, column: int) -> None:
        row = max(0, min(row, len(MNC_DEPLOY_FLAG_OPTION_ROWS) - 1))
        column = max(0, min(column, len(MNC_DEPLOY_FLAG_OPTION_ROWS[row]) - 1))
        option = MNC_DEPLOY_FLAG_OPTION_ROWS[row][column]
        self.screen.set_focus(self.query_one("#" + mnc_deploy_flag_checkbox_id(option.option_id), Checkbox))

    def move_deploy_flag_from(self, checkbox_id: Optional[str], row_delta: int = 0, col_delta: int = 0) -> bool:
        position = self._deploy_flag_position(checkbox_id)
        if position is None:
            return False

        row, column = position
        if row_delta < 0 and row == 0:
            self.focus_config_fields()
            return True
        if row_delta > 0 and row == len(MNC_DEPLOY_FLAG_OPTION_ROWS) - 1:
            return True

        target_row = max(0, min(row + row_delta, len(MNC_DEPLOY_FLAG_OPTION_ROWS) - 1))
        target_column = max(0, min(column + col_delta, len(MNC_DEPLOY_FLAG_OPTION_ROWS[target_row]) - 1))
        self.focus_deploy_flag(target_row, target_column)
        return True

    def _deploy_flag_position(self, checkbox_id: Optional[str]) -> Optional[tuple[int, int]]:
        option = mnc_deploy_flag_option_from_checkbox_id(checkbox_id)
        if option is None:
            return None
        for row_index, row in enumerate(MNC_DEPLOY_FLAG_OPTION_ROWS):
            for column_index, row_option in enumerate(row):
                if row_option.option_id == option.option_id:
                    return row_index, column_index
        return None
