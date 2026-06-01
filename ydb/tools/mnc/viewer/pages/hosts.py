from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.events import Key
from textual.widgets import Label

from ydb.tools.mnc.viewer.styles.common import status_class
from ydb.tools.mnc.viewer.styles.hosts import HOSTS_CSS
from ydb.tools.mnc.viewer.widgets.host_card import HostsContainer
from ydb.tools.mnc.viewer.widgets.state import ViewerState


class AgentsPane(VerticalScroll, inherit_bindings=False):
    DEFAULT_CSS = HOSTS_CSS

    def __init__(self, state: "ViewerState") -> None:
        super().__init__()
        self._state = state

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Hosts", id="agents-title"),
            Label(
                self._state.agents_status(),
                id="agents-status",
                classes=f"overview-status-value {status_class(self._state.agents_status_kind())}",
            ),
            id="agents-summary",
        )
        yield HostsContainer(self._state, id="agents-hosts")

    def refresh_state(self) -> None:
        agents_status = self.query_one("#agents-status", Label)
        agents_status.update(self._state.agents_status())
        agents_status.set_classes(f"overview-status-value {status_class(self._state.agents_status_kind())}")
        self.query_one("#agents-hosts", HostsContainer).refresh(recompose=True)

    def key_up(self, event: Key) -> None:
        self.scroll_up(animate=False, force=True)
        event.stop()

    def key_down(self, event: Key) -> None:
        self.scroll_down(animate=False, force=True)
        event.stop()
