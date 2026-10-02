import os
import shutil
from typing import Callable, Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Key
from textual.widgets import Label, ListItem, ListView, Static

from ydb.tools.mnc.viewer.styles.cluster_config import CLUSTER_CONFIG_CSS
from ydb.tools.mnc.viewer.widgets.config_models import (
    ConfigCandidate,
    ConfigValidation,
    SelectedClusterConfig,
    _config_candidate_name_from_path,
    _config_path_for_name,
    _validate_multinode_config,
)
from ydb.tools.mnc.viewer.widgets.modals import ConfigNameModal, MessageModal
from ydb.tools.mnc.viewer.widgets.state import ViewerState


class ConfigCandidateItem(ListItem):
    def __init__(self, candidate: ConfigCandidate) -> None:
        self.candidate = candidate
        super().__init__(
            Vertical(
                Label(candidate.name, classes="config-candidate-name"),
                Label(candidate.path, classes="config-candidate-path"),
                classes="config-candidate-content",
            ),
            classes="config-candidate-item",
        )


class ClusterConfigDetails(VerticalScroll, inherit_bindings=False):
    def compose(self) -> ComposeResult:
        yield Horizontal(
            Label("", id="cluster-config-details-name"),
            Label("SELECTED", id="cluster-config-selected-badge", classes="cluster-config-badge"),
            Label("OK", id="cluster-config-ok-badge", classes="cluster-config-badge"),
            Label("FAIL", id="cluster-config-fail-badge", classes="cluster-config-badge"),
            id="cluster-config-details-header",
        )
        yield Label("", id="cluster-config-details-path")
        yield Static("", id="cluster-config-validation-errors", markup=False)
        yield Static("", id="cluster-config-details-content", markup=False)

    def update_empty(self, message: str) -> None:
        self.query_one("#cluster-config-details-name", Label).update(message)
        self.query_one("#cluster-config-details-path", Label).update("")
        self.query_one("#cluster-config-validation-errors", Static).display = False
        self.query_one("#cluster-config-details-content", Static).update("")
        for badge_id in ("#cluster-config-selected-badge", "#cluster-config-ok-badge", "#cluster-config-fail-badge"):
            self.query_one(badge_id, Label).display = False
        self.scroll_to(y=0, animate=False, force=True)

    def update_candidate(
        self,
        candidate: ConfigCandidate,
        selected: bool,
        validation: ConfigValidation,
        content: str,
    ) -> None:
        self.query_one("#cluster-config-details-name", Label).update(candidate.name)
        self.query_one("#cluster-config-details-path", Label).update(candidate.path)

        self.query_one("#cluster-config-selected-badge", Label).display = selected
        self.query_one("#cluster-config-ok-badge", Label).display = validation.ok
        self.query_one("#cluster-config-fail-badge", Label).display = not validation.ok

        errors = self.query_one("#cluster-config-validation-errors", Static)
        if validation.errors:
            errors.display = True
            errors.update(
                "Config is not compatible with multinode scheme:\n"
                + "\n".join(f"- {error}" for error in validation.errors)
                + "\n"
            )
        else:
            errors.display = False
            errors.update("")

        self.query_one("#cluster-config-details-content", Static).update(content)
        self.scroll_to(y=0, animate=False, force=True)

    def key_up(self, event: Key) -> None:
        self.scroll_up(animate=False, force=True)
        event.stop()

    def key_down(self, event: Key) -> None:
        self.scroll_down(animate=False, force=True)
        event.stop()

    def key_left(self, event: Key) -> None:
        self.screen.query_one("#cluster-configs", ListView).focus()
        event.stop()


class ClusterConfigPane(Horizontal):
    BINDINGS = [
        Binding("r", "rename_config", "Rename"),
        Binding("e", "edit_config", "Edit"),
        Binding("c", "copy_config", "Copy"),
    ]

    DEFAULT_CSS = CLUSTER_CONFIG_CSS

    def __init__(
        self,
        candidates: list[ConfigCandidate],
        state: ViewerState,
        on_state_changed: Callable[[], None],
        load_candidates: Callable[[], list[ConfigCandidate]],
        edit_file: Callable[[str], Optional[str]],
    ) -> None:
        super().__init__()
        self._candidates = candidates
        self._state = state
        self._on_state_changed = on_state_changed
        self._load_candidates = load_candidates
        self._edit_file = edit_file

    def compose(self) -> ComposeResult:
        with Vertical(id="cluster-config-left"):
            yield ListView(id="cluster-configs")
        yield ClusterConfigDetails(id="cluster-config-details")

    def on_mount(self) -> None:
        self._refresh()
        self.query_one("#cluster-configs", ListView).focus()

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.list_view.id == "cluster-configs" and isinstance(event.item, ConfigCandidateItem):
            event.stop()
            self._show_details(event.item.candidate)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id == "cluster-configs" and isinstance(event.item, ConfigCandidateItem):
            event.stop()
            self._select_candidate(event.item.candidate)

    def on_key(self, event: Key) -> None:
        if event.key == "right" and self.screen.focused is self.query_one("#cluster-configs", ListView):
            self.query_one("#cluster-config-details", ClusterConfigDetails).focus()
            event.stop()

    def action_rename_config(self) -> None:
        candidate = self._highlighted_candidate()
        if candidate is None:
            return

        self.app.push_screen(
            ConfigNameModal("Rename config", candidate.name, "Rename"),
            lambda name: self._rename_candidate(candidate, name),
        )

    def action_copy_config(self) -> None:
        candidate = self._highlighted_candidate()
        if candidate is None:
            return

        self.app.push_screen(
            ConfigNameModal("Copy config", f"{candidate.name}_copy", "Copy"),
            lambda name: self._copy_candidate(candidate, name),
        )

    def action_edit_config(self) -> None:
        candidate = self._highlighted_candidate()
        if candidate is None:
            return

        error = self._edit_file(candidate.path)
        if error is not None:
            self._show_message("Editor failed", error)
            return

        self._reload_candidates(candidate.path)
        self._select_candidate_by_path(candidate.path)

    def _refresh(self, preferred_path: Optional[str] = None) -> None:
        list_view = self.query_one("#cluster-configs", ListView)
        list_view.clear()
        list_view.extend([ConfigCandidateItem(candidate) for candidate in self._candidates])
        selected_index = next(
            (
                index
                for index, candidate in enumerate(self._candidates)
                if candidate.path == preferred_path or self._state.is_selected_cluster_config(candidate)
            ),
            None,
        )
        list_view.index = selected_index if selected_index is not None else (0 if self._candidates else None)
        if self._candidates:
            self._show_details(self._candidates[list_view.index or 0])
        else:
            self.query_one("#cluster-config-details", ClusterConfigDetails).update_empty("No configs found")

    def _highlighted_candidate(self) -> Optional[ConfigCandidate]:
        item = self.query_one("#cluster-configs", ListView).highlighted_child
        if isinstance(item, ConfigCandidateItem):
            return item.candidate
        return None

    def _reload_candidates(self, preferred_path: Optional[str] = None) -> None:
        self._candidates = self._load_candidates()
        self._refresh(preferred_path)

    def _select_candidate_by_path(self, path: str) -> None:
        candidate = next((candidate for candidate in self._candidates if candidate.path == path), None)
        if candidate is None:
            if (
                self._state.selected_cluster_config is not None
                and self._state.selected_cluster_config.candidate.path == path
            ):
                self._state.selected_cluster_config = None
                self._on_state_changed()
            return
        self._select_candidate(candidate)

    def _select_candidate(self, candidate: ConfigCandidate) -> bool:
        validation = _validate_multinode_config(candidate.path)
        if not validation.ok:
            if self._state.is_selected_cluster_config(candidate):
                self._state.selected_cluster_config = None
                self._on_state_changed()
            self._show_details(candidate, validation)
            return False
        self._state.selected_cluster_config = SelectedClusterConfig(candidate, validation)
        self._on_state_changed()
        self._show_details(candidate, validation)
        return True

    def _rename_candidate(self, candidate: Optional[ConfigCandidate], name: Optional[str]) -> None:
        if candidate is None or name is None:
            return

        target_path = self._target_path(candidate, name)
        if target_path is None:
            return
        if target_path == candidate.path:
            return
        if os.path.exists(target_path):
            self._show_message("Rename failed", f"Config already exists:\n{target_path}")
            return

        os.rename(candidate.path, target_path)
        renamed = ConfigCandidate(_config_candidate_name_from_path(target_path), target_path)
        if self._state.is_selected_cluster_config(candidate):
            validation = _validate_multinode_config(target_path)
            self._state.selected_cluster_config = SelectedClusterConfig(renamed, validation)
            self._on_state_changed()
        self._reload_candidates(target_path)

    def _copy_candidate(self, candidate: Optional[ConfigCandidate], name: Optional[str]) -> None:
        if candidate is None or name is None:
            return

        target_path = self._target_path(candidate, name)
        if target_path is None:
            return
        if os.path.exists(target_path):
            self._show_message("Copy failed", f"Config already exists:\n{target_path}")
            return

        shutil.copy2(candidate.path, target_path)
        self._reload_candidates(target_path)

    def _target_path(self, candidate: ConfigCandidate, name: str) -> Optional[str]:
        if not name.strip():
            self._show_message("Invalid config name", "Config name must not be empty.")
            return None
        if os.path.basename(name) != name:
            self._show_message("Invalid config name", "Config name must not include directories.")
            return None
        return _config_path_for_name(candidate, name)

    def _show_message(self, title: str, message: str) -> None:
        self.app.push_screen(MessageModal(title, message))

    def _show_details(self, candidate: ConfigCandidate, validation: Optional[ConfigValidation] = None) -> None:
        try:
            with open(candidate.path) as file:
                content = file.read()
        except Exception as error:
            content = f"Failed to read config: {error}"
        validation = validation or _validate_multinode_config(candidate.path)
        self.query_one("#cluster-config-details", ClusterConfigDetails).update_candidate(
            candidate,
            self._state.is_selected_cluster_config(candidate),
            validation,
            content,
        )
