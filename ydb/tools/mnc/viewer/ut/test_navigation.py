import unittest
from dataclasses import dataclass

from textual.widgets import TabPane, TabbedContent

from ydb.tools.mnc.viewer.main import Viewer


TAB_IDS = ["general", "mnc-config", "cluster-config", "agents", "operation"]
EXHAUSTIVE_TAB_IDS = ["general", "mnc-config", "cluster-config"]
CORE_COMMAND_SEQUENCES = [
    ("previous_tab",),
    ("next_tab",),
    ("open_general",),
    ("open_mnc_config",),
    ("open_cluster_config",),
    ("close_tab",),
    ("open_mnc_config", "close_tab"),
    ("open_cluster_config", "close_tab"),
    ("next_tab", "previous_tab"),
    ("open_mnc_config", "open_cluster_config"),
    ("open_cluster_config", "open_mnc_config"),
]
AGENTS_COMMAND_SEQUENCES = [
    ("open_agents",),
    ("open_agents", "previous_tab"),
    ("open_agents", "next_tab"),
    ("open_agents", "close_tab"),
    ("open_agents", "open_mnc_config"),
    ("open_mnc_config", "open_agents"),
    ("open_cluster_config", "open_agents"),
]
OPERATION_COMMAND_SEQUENCES = [
    ("open_operation_install",),
    ("open_operation_install", "next_tab"),
    ("open_operation_install", "close_tab"),
    ("open_mnc_config", "open_operation_install"),
]


@dataclass
class TabState:
    tabs: list[str]
    active: str

    def apply(self, command: str) -> None:
        if command == "previous_tab":
            self._move(-1)
        elif command == "next_tab":
            self._move(1)
        elif command == "open_general":
            self.active = "general"
        elif command == "open_mnc_config":
            self._open("mnc-config")
        elif command == "open_cluster_config":
            self._open("cluster-config")
        elif command == "open_agents":
            self._open("agents")
        elif command == "open_operation_install":
            self._open("operation")
        elif command == "close_tab":
            self._close()
        else:
            raise AssertionError(f"unknown command: {command}")

    def _move(self, step: int) -> None:
        current_index = self.tabs.index(self.active) if self.active in self.tabs else 0
        self.active = self.tabs[(current_index + step) % len(self.tabs)]

    def _open(self, tab_id: str) -> None:
        if tab_id not in self.tabs:
            self.tabs.append(tab_id)
        self.active = tab_id

    def _close(self) -> None:
        if self.active == "general":
            return

        tab_index = self.tabs.index(self.active)
        self.tabs.remove(self.active)
        self.active = self.tabs[min(tab_index, len(self.tabs) - 1)]


class ViewerTabNavigationTest(unittest.IsolatedAsyncioTestCase):
    async def test_tab_navigation_matches_state_machine_for_core_command_sequences(self):
        for start_tab in EXHAUSTIVE_TAB_IDS:
            app = Viewer()
            async with app.run_test() as pilot:
                with self.subTest(start_tab=start_tab):
                    for commands in CORE_COMMAND_SEQUENCES:
                        await self._assert_command_sequence(
                            app,
                            pilot,
                            start_tab,
                            commands,
                            include_agents=False,
                        )

    async def test_agents_tab_navigation_matches_state_machine(self):
        for start_tab in ["general", "mnc-config", "cluster-config", "agents"]:
            app = Viewer()
            async with app.run_test() as pilot:
                with self.subTest(start_tab=start_tab):
                    for commands in AGENTS_COMMAND_SEQUENCES:
                        await self._assert_command_sequence(
                            app,
                            pilot,
                            start_tab,
                            commands,
                            include_agents=True,
                            include_operation=False,
                        )

    async def test_operation_tab_navigation_matches_state_machine(self):
        for start_tab in ["general", "operation"]:
            app = Viewer()
            async with app.run_test() as pilot:
                with self.subTest(start_tab=start_tab):
                    for commands in OPERATION_COMMAND_SEQUENCES:
                        await self._assert_command_sequence(
                            app,
                            pilot,
                            start_tab,
                            commands,
                            include_agents=True,
                            include_operation=True,
                        )

    async def _assert_command_sequence(
        self,
        app: Viewer,
        pilot,
        start_tab: str,
        commands: tuple[str, ...],
        include_agents: bool = True,
        include_operation: bool = False,
    ) -> None:
        await self._reset_tabs(app, start_tab, include_agents, include_operation)
        expected = TabState(self._actual_tabs(app), start_tab)
        self._assert_tabs(app, expected, commands, "initial")

        for command in commands:
            wait_for_close = command == "close_tab" and app.query_one("#tabs", TabbedContent).active != "general"
            await self._run_command(app, command)
            expected.apply(command)
            if wait_for_close:
                await self._wait_for_tab_panes(app, pilot, expected.tabs)
            self._assert_tabs(app, expected, commands, command)

    async def _reset_tabs(
        self,
        app: Viewer,
        start_tab: str,
        include_agents: bool = True,
        include_operation: bool = False,
    ) -> None:
        await self._run_command(app, "open_mnc_config")
        await self._run_command(app, "open_cluster_config")
        if include_agents:
            await self._run_command(app, "open_agents")
        if include_operation:
            await self._run_command(app, "open_operation_install")
        await self._run_open_tab(app, start_tab)
        await self._run_open_tab(app, start_tab)

    async def _run_command(self, app: Viewer, command: str) -> None:
        if command == "previous_tab":
            await app.run_action("previous_tab")
        elif command == "next_tab":
            await app.run_action("next_tab")
        elif command == "open_general":
            await self._run_open_tab(app, "general")
        elif command == "open_mnc_config":
            await app.run_action("open_mnc_config")
        elif command == "open_cluster_config":
            await app.run_action("open_cluster_config")
        elif command == "open_agents":
            await app.run_action("open_agents")
        elif command == "open_operation_install":
            await app.run_action("open_operation('install')")
        elif command == "close_tab":
            await app.run_action("close_tab")
        else:
            raise AssertionError(f"unknown command: {command}")

    async def _run_open_tab(self, app: Viewer, tab_id: str) -> None:
        if tab_id == "general":
            await app.run_action("show_tab('general')")
        elif tab_id == "mnc-config":
            await app.run_action("open_mnc_config")
        elif tab_id == "cluster-config":
            await app.run_action("open_cluster_config")
        elif tab_id == "agents":
            await app.run_action("open_agents")
        elif tab_id == "operation":
            await app.run_action("open_operation('install')")
        else:
            raise AssertionError(f"unknown tab: {tab_id}")

    def _actual_tabs(self, app: Viewer) -> list[str]:
        return [pane.id for pane in app.query_one("#tabs", TabbedContent).query(TabPane)]

    async def _wait_for_tab_panes(self, app: Viewer, pilot, expected_tabs: list[str]) -> None:
        for _ in range(10):
            if self._actual_tabs(app) == expected_tabs:
                return
            await pilot.pause()

    def _assert_tabs(
        self,
        app: Viewer,
        expected: TabState,
        commands: tuple[str, ...],
        step: str,
    ) -> None:
        tabs = app.query_one("#tabs", TabbedContent)
        actual_tabs = self._actual_tabs(app)
        message = f"commands={commands}, step={step}, opened={app._opened_tab_order}"

        self.assertEqual(actual_tabs, expected.tabs, message)
        self.assertEqual(tabs.tab_count, len(expected.tabs), message)
        self.assertEqual(tabs.active, expected.active, message)
