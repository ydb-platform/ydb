from typing import Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.events import Click, Key
from textual.widgets import Label, ListItem, ListView, Static

from ydb.tools.mnc.viewer.styles.common import status_class
from ydb.tools.mnc.viewer.styles.overview import OVERVIEW_CSS
from ydb.tools.mnc.viewer.widgets.state import ViewerState


class OverviewStatusCard(ListItem):
    def __init__(
        self,
        title: str,
        status: str,
        action: Optional[str] = None,
        id: Optional[str] = None,
        status_kind: Optional[str] = None,
    ) -> None:
        self.action = action
        self._status = status
        self._status_kind = status_kind or status
        self._status_label = Label(status, classes=f"overview-status-value {status_class(self._status_kind)}")
        super().__init__(
            Vertical(
                Label(title, classes="overview-status-title"),
                self._status_label,
                classes="overview-status-card-content",
            ),
            id=id,
            classes="overview-status-card",
        )

    @property
    def status(self) -> str:
        return self._status

    def update_status(self, status: str, status_kind: Optional[str] = None) -> None:
        self._status = status
        self._status_kind = status_kind or status
        self._status_label.update(status)
        self._status_label.set_classes(f"overview-status-value {status_class(self._status_kind)}")

    def _on_click(self, event: Click) -> None:
        if self.action is None:
            return
        event.stop()
        self.call_later(self.app.run_action, self.action)


class OverviewAgentsCard(ListItem):
    action = "open_agents"

    def __init__(self, state: "ViewerState") -> None:
        super().__init__(
            Vertical(
                Label("Hosts", id="overview-agents-title"),
                Label(
                    state.agents_status(),
                    id="overview-agents-status",
                    classes=f"overview-status-value {status_class(state.agents_status_kind())}",
                ),
                Static(state.agents_details(), id="overview-agents-hosts", markup=False),
                classes="overview-agents-card-content",
            ),
            id="overview-agents-card",
        )

    def _on_click(self, event: Click) -> None:
        event.stop()
        self.call_later(self.app.run_action, self.action)


class OverviewListView(ListView, can_focus=True, can_focus_children=False, inherit_bindings=False):
    BINDINGS = [
        Binding("enter", "select_cursor", "Select", show=False),
    ]


class OverviewStatusList(OverviewListView):
    def key_left(self, event: Key) -> None:
        if self.index is None:
            self.index = 0
        elif self.index > 0:
            self.index -= 1
        event.stop()

    def key_right(self, event: Key) -> None:
        if self.index is None:
            self.index = 0
        elif self.index < len(self.children) - 1:
            self.index += 1
        event.stop()

    def key_down(self, event: Key) -> None:
        self.parent.focus_agents_card()
        event.stop()


class OverviewAgentsList(OverviewListView):
    def key_up(self, event: Key) -> None:
        self.parent.focus_status_card()
        event.stop()


class OverviewPane(Vertical):
    DEFAULT_CSS = OVERVIEW_CSS

    def __init__(self, state: "ViewerState") -> None:
        super().__init__()
        self._state = state

    def compose(self) -> ComposeResult:
        yield OverviewStatusList(
            OverviewStatusCard(
                "Settings",
                self._state.mnc_config_status(),
                action="open_mnc_config",
                id="overview-mnc-config-card",
            ),
            OverviewStatusCard(
                "Cluster",
                self._state.cluster_config_status(),
                action="open_cluster_config",
                id="overview-cluster-config-card",
                status_kind=self._state.cluster_config_status_kind(),
            ),
            id="overview-status-row",
        )
        yield OverviewAgentsList(
            OverviewAgentsCard(self._state),
            initial_index=None,
            id="overview-agents-list",
        )

    def refresh_state(self) -> None:
        self.query_one("#overview-mnc-config-card", OverviewStatusCard).update_status(
            self._state.mnc_config_status()
        )
        self.query_one("#overview-cluster-config-card", OverviewStatusCard).update_status(
            self._state.cluster_config_status(),
            self._state.cluster_config_status_kind(),
        )
        agents_status = self.query_one("#overview-agents-status", Label)
        agents_status.update(self._state.agents_status())
        agents_status.set_classes(f"overview-status-value {status_class(self._state.agents_status_kind())}")
        self.query_one("#overview-agents-hosts", Static).update(self._state.agents_details())

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, (OverviewStatusCard, OverviewAgentsCard)):
            event.stop()
            if event.item.action is not None:
                await self.app.run_action(event.item.action)

    def focus_status_card(self) -> None:
        agents_list = self.query_one("#overview-agents-list", ListView)
        status_row = self.query_one("#overview-status-row", ListView)
        agents_list.index = None
        status_row.index = 0
        self.screen.set_focus(status_row)

    def focus_agents_card(self) -> None:
        status_row = self.query_one("#overview-status-row", ListView)
        agents_list = self.query_one("#overview-agents-list", ListView)
        status_row.index = None
        agents_list.index = 0
        self.screen.set_focus(agents_list)

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.item is None:
            return

        status_row = self.query_one("#overview-status-row", ListView)
        agents_list = self.query_one("#overview-agents-list", ListView)
        if event.list_view is status_row:
            agents_list.index = None
        elif event.list_view is agents_list:
            status_row.index = None

    def on_key(self, event: Key) -> None:
        focused = self.screen.focused
        status_row = self.query_one("#overview-status-row", ListView)
        agents_list = self.query_one("#overview-agents-list", ListView)

        if event.key in ("left", "right") and focused is status_row:
            if status_row.index is None:
                status_row.index = 0
            elif event.key == "left" and status_row.index > 0:
                status_row.index -= 1
            elif event.key == "right" and status_row.index < len(status_row.children) - 1:
                status_row.index += 1
            event.stop()
        elif event.key == "down" and focused is status_row:
            self.focus_agents_card()
            event.stop()
        elif event.key == "up" and focused is agents_list:
            self.focus_status_card()
            event.stop()
