from __future__ import annotations

import re

_WILDCARDS = {"*", "$"}


class _URLPattern:
    """Internal class which represents a URL pattern."""

    def __init__(self, pattern: str):
        self._pattern: str = pattern
        self.priority: int = len(pattern)
        self._contains_asterisk: bool = "*" in self._pattern
        self._contains_dollar: bool = self._pattern.endswith("$")

        if self._contains_asterisk:
            self._pattern_before_asterisk: str = self._pattern[
                : self._pattern.find("*")
            ]
        elif self._contains_dollar:
            self._pattern_before_dollar: str = self._pattern[:-1]

        self._pattern_compiled: re.Pattern[str] | None = None

    def match(self, url: str) -> bool:
        """Return True if pattern matches the given URL, otherwise return False."""
        # check if pattern is already compiled
        if self._pattern_compiled is not None:
            return bool(self._pattern_compiled.match(url))

        if not self._contains_asterisk:
            if not self._contains_dollar:
                # answer directly for patterns without wildcards
                return url.startswith(self._pattern)

            # pattern only contains $ wildcard.
            return url == self._pattern_before_dollar

        if not url.startswith(self._pattern_before_asterisk):
            return False

        _pattern_regex = self._prepare_pattern_for_regex(self._pattern)
        self._pattern_compiled = re.compile(_pattern_regex)
        return bool(self._pattern_compiled.match(url))

    @staticmethod
    def _prepare_pattern_for_regex(pattern: str) -> str:
        """Return equivalent regex pattern for the given URL pattern."""
        pattern = re.sub(r"\*+", "*", pattern)
        s = re.split(r"(\*|\$$)", pattern)
        for index, substr in enumerate(s):
            if substr not in _WILDCARDS:
                s[index] = re.escape(substr)
            elif s[index] == "*":
                s[index] = ".*?"
        return "".join(s)
