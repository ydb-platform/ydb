from textual.command import DiscoveryHit, Hit, Hits, Provider


class CommandProviderBase(Provider):
    def _commands(self) -> list[tuple[str, str, str]]:
        return []

    def _get_command_callback(self, action: str):
        if "(" in action:
            return lambda: self.app.run_action(action)
        return getattr(self.app, f"action_{action}")

    async def discover(self):
        for title, help_text, action in self._commands():
            yield DiscoveryHit(title, self._get_command_callback(action), help=help_text)

    async def search(self, query: str) -> Hits:
        matcher = self.matcher(query)
        for title, help_text, action in self._commands():
            score = matcher.match(title)
            if score > 0:
                yield Hit(
                    score,
                    matcher.highlight(title),
                    self._get_command_callback(action),
                    help=help_text,
                )


class TabCommands(CommandProviderBase):
    def _commands(self) -> list[tuple[str, str, str]]:
        return [
            (f"Open tab: {title}", description, f"show_tab('{tab_id}')")
            for tab_id, title, description in self.app.tab_choices(opened_only=True)
        ]


class OperationsCommands(CommandProviderBase):
    def _commands(self) -> list[tuple[str, str, str]]:
        return [
            (title, description, f"open_operation('{operation_id}')")
            for operation_id, title, description in self.app.operation_choices()
        ]


class ViewerCommands(CommandProviderBase):
    def _commands(self) -> list[tuple[str, str, str]]:
        return [
            ("Open tab: Overview", "Overview viewer and cluster state", "show_tab('general')"),
            ("Open tab: Settings", "Read and edit settings", "open_mnc_config"),
            ("Open tab: Cluster", "Select and inspect cluster", "open_cluster_config"),
            ("Open tab: Hosts", "Inspect selected cluster hosts", "open_agents"),
            ("Operations", "Select install or uninstall operation", "open_operations"),
            ("Close tab", "Close the current tab", "close_tab"),
        ]
