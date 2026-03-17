from __future__ import annotations

import logging
from typing import TYPE_CHECKING, NamedTuple

from ._urlpattern import _URLPattern
from ._utils import _hexescape, _parse_time_period, _quote_path, _quote_pattern

if TYPE_CHECKING:
    from datetime import time

    from ._protego import Protego


logger = logging.getLogger(__name__)


class RequestRate(NamedTuple):
    requests: int
    seconds: int
    start_time: time | None
    end_time: time | None


class VisitTime(NamedTuple):
    start_time: time
    end_time: time


class _Rule(NamedTuple):
    field: str
    value: _URLPattern


class _RuleSet:
    """Internal class which stores rules for a user agent."""

    def __init__(self, parser_instance: Protego):
        self.user_agent: str | None = None
        self._rules: list[_Rule] = []
        self._crawl_delay: float | None = None
        self._req_rate: RequestRate | None = None
        self._visit_time: VisitTime | None = None
        self._parser_instance: Protego = parser_instance

    def applies_to(self, robotname: str) -> int:
        """Return matching score."""
        assert self.user_agent is not None
        robotname = robotname.strip().lower()
        if self.user_agent == "*":
            return 1
        if self.user_agent in robotname:
            return len(self.user_agent)
        return 0

    def allow(self, pattern: str) -> None:
        if "$" in pattern:
            self.allow(pattern.replace("$", _hexescape("$")))

        pattern = _quote_pattern(pattern)
        if not pattern:
            return
        self._rules.append(_Rule(field="allow", value=_URLPattern(pattern)))

        # If index.html is allowed, we interpret this as / being allowed too.
        if pattern.endswith("/index.html"):
            self.allow(pattern[:-10] + "$")

    def disallow(self, pattern: str) -> None:
        if "$" in pattern:
            self.disallow(pattern.replace("$", _hexescape("$")))

        pattern = _quote_pattern(pattern)
        if not pattern:
            return
        self._rules.append(_Rule(field="disallow", value=_URLPattern(pattern)))

    def finalize_rules(self) -> None:
        self._rules.sort(
            key=lambda r: (r.value.priority, r.field == "allow"), reverse=True
        )

    def can_fetch(self, url: str) -> bool:
        """Return if the url can be fetched."""
        url = _quote_path(url)
        allowed = True
        for rule in self._rules:
            if rule.value.match(url):
                if rule.field == "disallow":
                    allowed = False
                break
        return allowed

    @property
    def crawl_delay(self) -> float | None:
        """Get & set crawl delay for the rule set."""
        return self._crawl_delay

    @crawl_delay.setter
    def crawl_delay(self, delay: str) -> None:
        try:
            self._crawl_delay = float(delay)
        except ValueError:
            # Value is malformed, do nothing.
            logger.debug(
                f"Malformed rule at line {self._parser_instance._total_line_seen} : "
                f"cannot set crawl delay to '{delay}'. Ignoring this rule."
            )

    @property
    def request_rate(self) -> RequestRate | None:
        """Get & set request rate for the rule set."""
        return self._req_rate

    @request_rate.setter
    def request_rate(self, value: str) -> None:
        try:
            parts = value.split()
            if len(parts) == 2:
                rate, time_period = parts
            else:
                rate, time_period = parts[0], ""

            requests_str, seconds_str = rate.split("/")
            time_unit = seconds_str[-1].lower()
            requests, seconds = int(requests_str), int(seconds_str[:-1])

            if time_unit == "m":
                seconds *= 60
            elif time_unit == "h":
                seconds *= 3600
            elif time_unit == "d":
                seconds *= 86400

            start_time = None
            end_time = None
            if time_period:
                start_time, end_time = _parse_time_period(time_period)
        except Exception:
            # Value is malformed, do nothing.
            logger.debug(
                f"Malformed rule at line {self._parser_instance._total_line_seen} : "
                f"cannot set request rate using '{value}'. Ignoring this rule."
            )
            return

        self._req_rate = RequestRate(requests, seconds, start_time, end_time)

    @property
    def visit_time(self) -> VisitTime | None:
        """Get & set visit time for the rule set."""
        return self._visit_time

    @visit_time.setter
    def visit_time(self, value: str) -> None:
        try:
            start_time, end_time = _parse_time_period(value, separator=" ")
        except Exception:
            logger.debug(
                f"Malformed rule at line {self._parser_instance._total_line_seen} : "
                f"cannot set visit time using '{value}'. Ignoring this rule."
            )
            return
        self._visit_time = VisitTime(start_time, end_time)
