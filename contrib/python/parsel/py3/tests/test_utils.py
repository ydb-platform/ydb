from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from parsel.utils import extract_regex, shorten

if TYPE_CHECKING:
    from re import Pattern


@pytest.mark.parametrize(
    ("text", "width", "suffix", "expected"),
    [
        ("foobar", -1, "...", ValueError),
        ("foobar", 0, "...", ""),
        ("foobar", 1, "...", "."),
        ("foobar", 2, "...", ".."),
        ("foobar", 3, "...", "..."),
        ("foobar", 4, "...", "f..."),
        ("foobar", 5, "...", "fo..."),
        ("foobar", 6, "...", "foobar"),
        ("foobar", 7, "...", "foobar"),
        ("hello", 3, "…", "he…"),
        ("hello", 4, "…", "hel…"),
        ("test", 2, "->", "->"),
        ("test", 3, "->", "t->"),
        ("test", 4, "->", "test"),
        ("", 0, "...", ""),
        ("", 3, "...", ""),
    ],
)
def test_shorten(
    text: str, width: int, suffix: str, expected: str | type[Exception]
) -> None:
    if isinstance(expected, str):
        assert shorten(text, width, suffix=suffix) == expected
    else:
        with pytest.raises(expected):
            shorten(text, width, suffix=suffix)


@pytest.mark.parametrize(
    ("regex", "text", "replace_entities", "expected"),
    [
        (
            r"(?P<month>\w+)\s*(?P<day>\d+)\s*\,?\s*(?P<year>\d+)",
            "October  25, 2019",
            True,
            ["October", "25", "2019"],
        ),
        (
            r"(?P<month>\w+)\s*(?P<day>\d+)\s*\,?\s*(?P<year>\d+)",
            "October  25 2019",
            True,
            ["October", "25", "2019"],
        ),
        (
            r"(?P<extract>\w+)\s*(?P<day>\d+)\s*\,?\s*(?P<year>\d+)",
            "October  25 2019",
            True,
            ["October"],
        ),
        (
            r"\w+\s*\d+\s*\,?\s*\d+",
            "October  25 2019",
            True,
            ["October  25 2019"],
        ),
        (
            r"^.*$",
            "&quot;sometext&quot; &amp; &quot;moretext&quot;",
            True,
            ['"sometext" &amp; "moretext"'],
        ),
        (
            r"^.*$",
            "&quot;sometext&quot; &amp; &quot;moretext&quot;",
            False,
            ["&quot;sometext&quot; &amp; &quot;moretext&quot;"],
        ),
        (
            r"(?P<extract>\d+)",
            "no digits here",
            True,
            [],
        ),
    ],
)
def test_extract_regex(
    regex: str | Pattern[str],
    text: str,
    replace_entities: bool,
    expected: list[str],
) -> None:
    assert extract_regex(regex, text, replace_entities) == expected
