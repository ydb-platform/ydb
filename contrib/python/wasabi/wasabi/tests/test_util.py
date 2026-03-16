import pytest

from wasabi.util import color, diff_strings, format_repr, locale_escape, wrap


def test_color():
    assert color("test", fg="green") == "\x1b[38;5;2mtest\x1b[0m"
    assert color("test", fg=4) == "\x1b[38;5;4mtest\x1b[0m"
    assert color("test", bold=True) == "\x1b[1mtest\x1b[0m"
    assert color("test", fg="red", underline=True) == "\x1b[4;38;5;1mtest\x1b[0m"
    assert (
        color("test", fg=7, bg="red", bold=True) == "\x1b[1;38;5;7;48;5;1mtest\x1b[0m"
    )


def test_wrap():
    text = "Hello world, this is a test."
    assert wrap(text, indent=0) == text
    assert wrap(text, indent=4) == "    Hello world, this is a test."
    assert wrap(text, wrap_max=10, indent=0) == "Hello\nworld,\nthis is a\ntest."
    assert (
        wrap(text, wrap_max=5, indent=2)
        == "  Hello\n  world,\n  this\n  is\n  a\n  test."
    )


def test_format_repr():
    obj = {"hello": "world", "test": 123}
    formatted = format_repr(obj)
    assert formatted.replace("u'", "'") in [
        "{'hello': 'world', 'test': 123}",
        "{'test': 123, 'hello': 'world'}",
    ]
    formatted = format_repr(obj, max_len=10)
    assert formatted.replace("u'", "'") in [
        "{'hel ...  123}",
        "{'tes ... rld'}",
        "{'te ... rld'}",
    ]
    formatted = format_repr(obj, max_len=10, ellipsis="[...]")
    assert formatted.replace("u'", "'") in [
        "{'hel [...]  123}",
        "{'tes [...] rld'}",
        "{'te [...] rld'}",
    ]


@pytest.mark.parametrize(
    "text,non_ascii",
    [
        ("abc", ["abc"]),
        ("\u2714 abc", ["? abc"]),
        ("👻", ["??", "?"]),  # On Python 3 windows, this becomes "?" instead of "??"
    ],
)
def test_locale_escape(text, non_ascii):
    result = locale_escape(text)
    assert result == text or result in non_ascii
    print(result)


def test_diff_strings():
    a = "hello\nworld\nwide\nweb"
    b = "yo\nwide\nworld\nweb"
    expected = "\x1b[38;5;16;48;5;2myo\x1b[0m\n\x1b[38;5;16;48;5;2mwide\x1b[0m\n\x1b[38;5;16;48;5;1mhello\x1b[0m\nworld\n\x1b[38;5;16;48;5;1mwide\x1b[0m\nweb"
    assert diff_strings(a, b) == expected


def test_diff_strings_with_symbols():
    a = "hello\nworld\nwide\nweb"
    b = "yo\nwide\nworld\nweb"
    expected = "\x1b[38;5;16;48;5;2m+ yo\x1b[0m\n\x1b[38;5;16;48;5;2m+ wide\x1b[0m\n\x1b[38;5;16;48;5;1m- hello\x1b[0m\nworld\n\x1b[38;5;16;48;5;1m- wide\x1b[0m\nweb"
    assert diff_strings(a, b, add_symbols=True) == expected
