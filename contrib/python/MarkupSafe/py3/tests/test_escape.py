from __future__ import annotations

import typing as t

import pytest

from markupsafe import escape
from markupsafe import Markup


@pytest.mark.parametrize(
    ("value", "expect"),
    (
        # empty
        ("", ""),
        # ascii
        ("abcd&><'\"efgh", "abcd&amp;&gt;&lt;&#39;&#34;efgh"),
        ("&><'\"efgh", "&amp;&gt;&lt;&#39;&#34;efgh"),
        ("abcd&><'\"", "abcd&amp;&gt;&lt;&#39;&#34;"),
        # 2 byte
        ("こんにちは&><'\"こんばんは", "こんにちは&amp;&gt;&lt;&#39;&#34;こんばんは"),
        ("&><'\"こんばんは", "&amp;&gt;&lt;&#39;&#34;こんばんは"),
        ("こんにちは&><'\"", "こんにちは&amp;&gt;&lt;&#39;&#34;"),
        # 4 byte
        (
            "\U0001f363\U0001f362&><'\"\U0001f37a xyz",
            "\U0001f363\U0001f362&amp;&gt;&lt;&#39;&#34;\U0001f37a xyz",
        ),
        ("&><'\"\U0001f37a xyz", "&amp;&gt;&lt;&#39;&#34;\U0001f37a xyz"),
        ("\U0001f363\U0001f362&><'\"", "\U0001f363\U0001f362&amp;&gt;&lt;&#39;&#34;"),
    ),
)
def test_escape(value: str, expect: str) -> None:
    assert escape(value) == Markup(expect)


class Proxy:
    def __init__(self, value: t.Any) -> None:
        self.__value = value

    @property  # type: ignore[misc]
    def __class__(self) -> type[t.Any]:
        # Make o.__class__ and isinstance(o, str) see the proxied object.
        return self.__value.__class__  # type: ignore[no-any-return]

    def __str__(self) -> str:
        return str(self.__value)


def test_proxy() -> None:
    """Handle a proxy object that pretends its __class__ is str."""
    p = Proxy("test")
    assert p.__class__ is str
    assert isinstance(p, str)
    assert escape(p) == Markup("test")


class ReferenceStr(str):
    def __str__(self) -> str:
        # This should return a str, but it returns the subclass instead.
        return self


def test_subclass() -> None:
    """Handle if str(o) does not return a plain str."""
    s = ReferenceStr("test")
    assert isinstance(s, str)
    assert escape(s) == Markup("test")
