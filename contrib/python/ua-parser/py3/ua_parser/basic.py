__all__ = ["Resolver"]

import re
from itertools import chain
from operator import methodcaller
from typing import Any, List

from .core import (
    Device,
    Domain,
    Matcher,
    Matchers,
    OS,
    PartialResult,
    UserAgent,
)
from .utils import IS_GRAAL, fa_simplifier


class Resolver:
    """A simple pure-python resolver based around trying a number of
    regular expressions in sequence for each domain, and returning a
    result when one matches.

    """

    user_agent_matchers: List[Matcher[UserAgent]]
    os_matchers: List[Matcher[OS]]
    device_matchers: List[Matcher[Device]]

    def __init__(
        self,
        matchers: Matchers,
    ) -> None:
        self.user_agent_matchers, self.os_matchers, self.device_matchers = matchers
        if IS_GRAAL:
            matcher: Any
            kind = next(
                (
                    "eager" if hasattr(type(m), "regex") else "lazy"
                    for m in chain.from_iterable(matchers)
                ),
                None,
            )
            if kind == "eager":
                for matcher in chain.from_iterable(matchers):
                    matcher.pattern = re.compile(
                        fa_simplifier(matcher.pattern.pattern),
                        flags=matcher.pattern.flags,
                    )
            elif kind == "lazy":
                for matcher in chain.from_iterable(matchers):
                    matcher.regex = fa_simplifier(matcher.pattern.pattern)

    def __call__(self, ua: str, domains: Domain, /) -> PartialResult:
        parse = methodcaller("__call__", ua)
        return PartialResult(
            domains=domains,
            string=ua,
            user_agent=(
                next(
                    filter(None, map(parse, self.user_agent_matchers)),
                    None,
                )
                if Domain.USER_AGENT in domains
                else None
            ),
            os=(
                next(
                    filter(None, map(parse, self.os_matchers)),
                    None,
                )
                if Domain.OS in domains
                else None
            ),
            device=(
                next(
                    filter(None, map(parse, self.device_matchers)),
                    None,
                )
                if Domain.DEVICE in domains
                else None
            ),
        )
