from textual.command import DiscoveryHit, Hit, Hits, Provider


class ViewerCommands(Provider):
    def _commands(self) -> list[tuple[str, str, str]]:
        return [
            ("Open tab: MNC Config", "Open the MNC Config tab", "open_mnc_config"),
            ("Close tab", "Close the current tab", "close_tab"),
        ]

    async def discover(self):
        app = self.app
        for title, help_text, action in self._commands():
            yield DiscoveryHit(title, getattr(app, f"action_{action}"), help=help_text)

    async def search(self, query: str) -> Hits:
        matcher = self.matcher(query)
        app = self.app
        for title, help_text, action in self._commands():
            score = matcher.match(title)
            if score > 0:
                yield Hit(
                    score,
                    matcher.highlight(title),
                    getattr(app, f"action_{action}"),
                    help=help_text,
                )
