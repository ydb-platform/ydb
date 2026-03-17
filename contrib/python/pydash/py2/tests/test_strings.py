# -*- coding: utf-8 -*-

import re
import sys

import pytest

import pydash as _
from pydash._compat import parse_qsl, urlsplit

from .fixtures import parametrize


@parametrize(
    "case,expected",
    [
        ("foo bar baz", "fooBarBaz"),
        ("foo  bar baz", "fooBarBaz"),
        ("foo__bar_baz", "fooBarBaz"),
        ("foo-_bar-_-baz", "fooBarBaz"),
        ("foo!bar,baz", "fooBarBaz"),
        ("--foo.bar;baz", "fooBarBaz"),
        (8, "8"),
        ("", ""),
        (None, ""),
    ],
)
def test_camel_case(case, expected):
    assert _.camel_case(case) == expected


@parametrize(
    "case,expected",
    [
        (("foo",), "Foo"),
        (("foo bar",), "Foo bar"),
        (("fOO bar",), "Foo bar"),
        (("foo Bar",), "Foo bar"),
        (("foo", False), "Foo"),
        (("foo bar", False), "Foo bar"),
        (("fOO bar", False), "FOO bar"),
        (("foo Bar", False), "Foo Bar"),
        ((8,), "8"),
        ((" ",), " "),
        (("",), ""),
        ((None,), ""),
    ],
)
def test_capitalize(case, expected):
    assert _.capitalize(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foobarbaz", 3), ["foo", "bar", "baz"]),
        (("foobarbazaa", 3), ["foo", "bar", "baz", "aa"]),
        (("foo", 4), ["foo"]),
        (("foo", 0), ["foo"]),
        (("", 3), []),
        (("foo", -2), ["foo"]),
        ((None, 0), []),
        ((None, 1), []),
        ((None, -1), []),
    ],
)
def test_chop(case, expected):
    assert _.chop(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foobarbaz", 3), ["foo", "bar", "baz"]),
        (("foobarbazaa", 3), ["fo", "oba", "rba", "zaa"]),
        (("foo", 4), ["foo"]),
        (("foo", 0), ["foo"]),
        (("", 3), []),
        (("foo", -2), ["foo"]),
        ((None, 0), []),
        ((None, 1), []),
        ((None, -1), []),
    ],
)
def test_chop_right(case, expected):
    assert _.chop_right(*case) == expected


@parametrize(
    "case,expected",
    [
        ("  foo bar", "foo bar"),
        ("  foo  bar", "foo bar"),
        ("  foo  bar  ", "foo bar"),
        ("", ""),
        (None, ""),
    ],
)
def test_clean(case, expected):
    assert _.clean(case) == expected


@parametrize(
    "case,expected",
    [
        ("foobar", ["f", "o", "o", "b", "a", "r"]),
        ("", []),
        (5, ["5"]),
        (-5.6, ["-", "5", ".", "6"]),
        (None, []),
    ],
)
def test_chars(case, expected):
    assert _.chars(case) == expected


@parametrize(
    "case,expected",
    [
        (("foobar", "o"), 2),
        (("foobar", "oo"), 1),
        (("foobar", "ooo"), 0),
        (("", "ooo"), 0),
        (("", None), 0),
        ((None, "ooo"), 0),
        ((None, None), 0),
        ((5, None), 0),
        ((5, 5), 1),
        ((5.5, 5), 2),
        ((5.5, "5."), 1),
        ((65.5, "5."), 1),
        ((654.5, "5."), 0),
        (("", ""), 1),
        (("1", ""), 2),
        ((1.4, ""), 4),
    ],
)
def test_count_substr(case, expected):
    assert _.count_substr(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            "\xC0\xC1\xC2\xC3\xC4\xC5\xC6\xC7\xC8\xC9\xCA\xCB\xCC\xCD\xCE\xCF"
            "\xD0\xD1\xD2\xD3\xD4\xD5\xD6\xD7\xD8\xD9\xDA\xDB\xDC\xDD\xDE\xDF"
            "\xE0\xE1\xE2\xE3\xE4\xE5\xE6\xE7\xE8\xE9\xEA\xEB\xEC\xED\xEE\xEF"
            "\xF0\xF1\xF2\xF3\xF4\xF5\xF6\xF7\xF8\xF9\xFA\xFB\xFC\xFD\xFE\xFF",
            "AAAAAAAeCEEEEIIII" "DNOOOOO OUUUUYThss" "aaaaaaaeceeeeiiii" "dnooooo ouuuuythy",
        ),
        ("abcABC", "abcABC"),
        ("", ""),
        (None, ""),
    ],
)
def test_deburr(case, expected):
    assert _.deburr(case) == expected


@parametrize(
    "case,expected",
    [
        ("Foo", "foo"),
        ("Foo bar", "foo bar"),
        ("Foo Bar", "foo Bar"),
        ("FOO BAR", "fOO BAR"),
        (None, ""),
    ],
)
def test_decapitalize(case, expected):
    assert _.decapitalize(case) == expected


@parametrize(
    "case,expected",
    [
        (("abc", "c"), True),
        (("abc", "b"), False),
        (("abc", None), True),
        (("", "b"), False),
        (("", None), True),
        ((None, "b"), False),
        ((None, None), True),
        ((6.34, 4), True),
        ((6.34, 3), False),
        (("abc", "c", 3), True),
        (("abc", "c", 2), False),
        (("abc", "b", 2), True),
        (("abc", "b", 1), False),
        ((6.34, "b", 1), False),
    ],
)
def test_ends_with(case, expected):
    assert _.ends_with(*case) == expected


@parametrize(
    "case,expected",
    [
        ("abc<> &\"'`efg", "abc&lt;&gt; &amp;&quot;&#39;&#96;efg"),
        ("abc", "abc"),
        ("", ""),
        (None, ""),
    ],
)
def test_escape(case, expected):
    assert _.escape(case) == expected


@parametrize(
    "case,expected",
    [
        ("abc", "abc"),
        ("", ""),
        (None, ""),
    ],
)
def test_escape_reg_exp(case, expected):
    assert _.escape_reg_exp(case) == expected


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
@parametrize(
    "case,expected",
    [  # noqa
        (
            "[pydash](http://pydash.readthedocs.org/)",
            "\\[pydash\\]\\(http://pydash\\.readthedocs\\.org/\\)",
        ),
    ],
)
def test_escape_reg_exp_gte_py37(case, expected):
    assert _.escape_reg_exp(case) == expected


@pytest.mark.skipif(sys.version_info >= (3, 7), reason="requires python3.6 or lower")
@parametrize(
    "case,expected",
    [  # noqa
        (
            "[pydash](http://pydash.readthedocs.org/)",  # noqa
            r"\[pydash\]\(http\:\/\/pydash\.readthedocs\.org\/\)",
        ),
    ],
)
def test_escape_reg_exp_lte_py36(case, expected):
    assert _.escape_reg_exp(case) == expected


@parametrize(
    "text,prefix,expected",
    [
        ("Hello world!", "Hello", "Hello world!"),
        (" world!", "Hello", "Hello world!"),
        ("", "Hello", "Hello"),
        ("", "", ""),
        ("1", "", "1"),
        ("1", "1", "1"),
        (5, 6, "65"),
        (None, 6, "6"),
        (None, None, ""),
    ],
)
def test_ensure_starts_with(text, prefix, expected):
    assert _.ensure_starts_with(text, prefix) == expected


@parametrize(
    "text,suffix,expected",
    [
        ("Hello world!", "world!", "Hello world!"),
        ("Hello ", "world!", "Hello world!"),
        ("", "Hello", "Hello"),
        ("", "", ""),
        ("1", "", "1"),
        ("1", "1", "1"),
        (5, 6, "56"),
        (None, 6, "6"),
        (None, None, ""),
    ],
)
def test_ensure_ends_with(text, suffix, expected):
    assert _.ensure_ends_with(text, suffix) == expected


@parametrize(
    "case,expected",
    [
        (("foobar", "oo"), True),
        (("foobar", "x"), False),
        (("foobar", "f"), True),
        (("foobar", "r"), True),
        (("foobar", ""), True),
        (("foobar", None), True),
        (("", ""), True),
        (("", None), True),
        ((None, None), True),
        ((56, 6), True),
        ((56, 7), False),
        ((5.6, "."), True),
    ],
)
def test_has_substr(case, expected):
    assert _.has_substr(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            "  capitalize dash-CamelCase_underscore trim_id  ",
            "Capitalize dash camel case underscore trim",
        ),
        ("foo_bar_id", "Foo bar"),
        ("FooBar", "Foo bar"),
        (5, "5"),
        ("", ""),
        (None, ""),
    ],
)
def test_human_case(case, expected):
    assert _.human_case(case) == expected


@parametrize(
    "case,expected",
    [
        (("foobar", 0, "xx"), "xxfoobar"),
        (("foobar", 1, "xx"), "fxxoobar"),
        (("foobar", 4, "xx"), "foobxxar"),
        (("foobar", 6, "xx"), "foobarxx"),
        (("foobar", 7, "xx"), "foobarxx"),
        (("f", 7, "xx"), "fxx"),
        (("", 7, "xx"), "xx"),
        (("", 7, ""), ""),
        (("", 7, None), ""),
        ((None, 7, None), ""),
        ((None, 0, None), ""),
    ],
)
def test_insert_substr(case, expected):
    assert _.insert_substr(*case) == expected


@parametrize(
    "case,expected",
    [
        (((1, 2, 3), "."), "1.2.3"),
        ((("one", "two", "three"), "-.-"), "one-.-two-.-three"),
        ((("s", "t", "r", "i", "n", "g"), ""), "string"),
        ((("s", "t", "r", "i", "n", "g"),), "string"),
        ((("s", "t", "r", "i", "n", "g"), None), "string"),
        ((("string1", "string2"), ","), "string1,string2"),
        ((("string1", "string2"), 5.6), "string15.6string2"),
        (((None, "string2"), 5.6), "5.6string2"),
        (((7,), 5.6), "7"),
        (((None,), 5.6), ""),
        (((None, None), 5.6), "5.6"),
        (((None, None, None), 5.6), "5.65.6"),
        (((None, None, None), None), ""),
        ((None, None), ""),
    ],
)
def test_join(case, expected):
    assert _.join(*case) == expected


@parametrize(
    "case,expected",
    [
        ("foo  bar baz", "foo-bar-baz"),
        ("foo__bar_baz", "foo-bar-baz"),
        ("foo-_bar-_-baz", "foo-bar-baz"),
        ("foo!bar,baz", "foo-bar-baz"),
        ("--foo.bar;baz", "foo-bar-baz"),
        ("Foo Bar", "foo-bar"),
        ("fooBar", "foo-bar"),
        (None, ""),
        (5, "5"),
        (5.6, "5-6"),
        (-5.6, "5-6"),
    ],
)
def test_kebab_case(case, expected):
    assert _.kebab_case(case) == expected


@parametrize(
    "case,expected",
    [
        ("foo\nbar", ["foo", "bar"]),
        ("foo\rbar", ["foo", "bar"]),
        ("foo\r\nbar", ["foo", "bar"]),
        ("foo\n", ["foo"]),
        ("\nfoo", ["", "foo"]),
        ("", []),
        (None, []),
    ],
)
def test_lines(case, expected):
    assert _.lines(case) == expected


@parametrize(
    "case,expected",
    [  # noqa
        ("fooBar", "foo bar"),
        ("--foo-Bar--", "foo bar"),
        ("*Foo*B_a*R", "foo b a r"),
        ("*Foo10_B*Ar", "foo 10 b ar"),
        ('/?*Foo10/;"B*Ar', "foo 10 b ar"),
        ('/?F@O__o10456?>.B?>";Ar', "f o o 10456 b ar"),
        ("FoO Bar", "fo o bar"),
        ("F\n\\soO Bar", "f so o bar"),
        ("Foo1054665Bar", "foo 1054665 bar"),
    ],
)
def test_lower_case(case, expected):
    assert _.lower_case(case) == expected


@parametrize(
    "case,expected",
    [("Foobar", "foobar"), ("Foo Bar", "foo Bar"), ("1foobar", "1foobar"), (";foobar", ";foobar")],
)
def test_lower_first(case, expected):
    assert _.lower_first(case) == expected


@parametrize(
    "case,expected",
    [
        ((1234,), "1,234"),
        ((1234567890,), "1,234,567,890"),
        ((1234, 2), "1,234.00"),
        ((1234, 1), "1,234.0"),
        ((1234, 2, ",", "."), "1.234,00"),
        (("1234",), ""),
    ],
)
def test_number_format(case, expected):
    assert _.number_format(*case) == expected


@parametrize(
    "case,expected",
    [
        (("abc", 5, "12"), "1abc1"),
        (("abc", 8), "  abc   "),
        (("abc", 8, "_-"), "_-abc_-_"),
        (("abc", 3), "abc"),
        (("", 3), "   "),
        ((" ", 3), "   "),
        ((None, 3), "   "),
    ],
)
def test_pad(case, expected):
    assert _.pad(*case) == expected


@parametrize(
    "case,expected",
    [
        (("aaaaa", 3), "aaaaa"),
        (("aaaaa", 6), " aaaaa"),
        (("aaaaa", 10), "     aaaaa"),
        (("aaaaa", 6, "b"), "baaaaa"),
        (("aaaaa", 6, "bc"), "caaaaa"),
        (("aaaaa", 9, "bc"), "bcbcaaaaa"),
        (("a", 9, "12"), "12121212a"),
        (("a", 8, "12"), "2121212a"),
        (("", 8, "12"), "12121212"),
        ((None, 8, "12"), "12121212"),
    ],
)
def test_pad_start(case, expected):
    assert _.pad_start(*case) == expected


@parametrize(
    "case,expected",
    [
        (("aaaaa", 3), "aaaaa"),
        (("aaaaa", 6), "aaaaa "),
        (("aaaaa", 10), "aaaaa     "),
        (("aaaaa", 6, "b"), "aaaaab"),
        (("aaaaa", 6, "bc"), "aaaaab"),
        (("aaaaa", 9, "bc"), "aaaaabcbc"),
        (("a", 9, "12"), "a12121212"),
        (("a", 8, "12"), "a1212121"),
        (("", 8, "12"), "12121212"),
        ((None, 8, "12"), "12121212"),
    ],
)
def test_pad_end(case, expected):
    assert _.pad_end(*case) == expected


@parametrize(
    "case,expected",
    [
        ("foo bar baz", "FooBarBaz"),
        ("foo  bar baz", "FooBarBaz"),
        ("foo__bar_baz", "FooBarBaz"),
        ("foo-_bar-_-baz", "FooBarBaz"),
        ("foo!bar,baz", "FooBarBaz"),
        ("--foo.bar;baz", "FooBarBaz"),
        ("", ""),
        (None, ""),
    ],
)
def test_pascal_case(case, expected):
    assert _.pascal_case(case) == expected


@parametrize(
    "case,expected",
    [
        ("b", "a"),
        ("B", "A"),
    ],
)
def test_predecessor(case, expected):
    assert _.predecessor(case) == expected


@parametrize(
    "case,expected",
    [
        (("Hello, world",), "..."),
        (("Hello, world", 5), "Hello..."),
        (("Hello, world", 8), "Hello..."),
        (("Hello, world", 5, " (read a lot more)"), "Hello, world"),
        (("Hello, cruel world", 15), "Hello, cruel..."),
        (("Hello", 10), "Hello"),
        (("", 10), ""),
        (("",), ""),
        ((None,), ""),
    ],
)
def test_prune(case, expected):
    assert _.prune(*case) == expected


@parametrize(
    "case,expected",
    [
        (("hello world!",), '"hello world!"'),
        (("",), '""'),
        (("", None), ""),
        ((None,), '""'),
        ((None, None), ""),
        ((5,), '"5"'),
        ((5, None), "5"),
        ((-89,), '"-89"'),
        (("hello world!", "*"), "*hello world!*"),
        (("hello world!", "**"), "**hello world!**"),
        (("", "**"), "****"),
        (("hello world!", ""), "hello world!"),
        ((28, "**"), "**28**"),
        ((28, ""), "28"),
        ((2, 8), "828"),
        ((-2, 8), "8-28"),
        ((-2, -8), "-8-2-8"),
        ((-8.5, 0), "0-8.50"),
    ],
)
def test_quote(case, expected):
    assert _.quote(*case) == expected


@parametrize(
    "case,expected",
    [
        (("Hello World", "/[A-Z]/"), ["H"]),
        (("Hello World", "/[A-Z]/g"), ["H", "W"]),
        (("hello world", "/[A-Z]/i"), ["h"]),
        (("hello world", "/[A-Z]/gi"), ["h", "e", "l", "l", "o", "w", "o", "r", "l", "d"]),
        (("12345", "/[A-Z]/"), []),
    ],
)
def test_reg_exp_js_match(case, expected):
    assert _.reg_exp_js_match(*case) == expected


@parametrize(
    "case,expected",
    [
        (("Hello World", "/[A-Z]/", "!"), "!ello World"),
        (("Hello World", "/[A-Z]/g", "!"), "!ello !orld"),
        (("hello world", "/[A-Z]/i", "!"), "!ello world"),
        (("hello world", "/[A-Z]/gi", "!"), "!!!!! !!!!!"),
        (("hello world", "/[A-Z]/gi", ""), " "),
        (("hello world", "/[A-Z]/gi", None), " "),
    ],
)
def test_reg_exp_js_replace(case, expected):
    assert _.reg_exp_js_replace(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foo", "o", "a"), "faa"),
        (("foo", "o", "a", False, 1), "fao"),
        (("fOO", "o", "a"), "fOO"),
        (("fOO", "o", "a", True), "faa"),
        (("", "", ""), ""),
        (("foo", "o", ""), "f"),
        (("foo", "x", "y"), "foo"),
        (("foo", "^o", "a"), "foo"),
        (("ooo", "^o", "f"), "foo"),
        (("foo", "o$", "a"), "foa"),
        (("foo", "", "a"), "afaoaoa"),
        (("foo", "", ""), "foo"),
        (("foo", "", None), "foo"),
        (("foo", None, None), "foo"),
        (("foo", None, ""), "foo"),
        (("foo", None, "a"), "foo"),
        ((54.7, 5, 6), "64.7"),
    ],
)
def test_reg_exp_replace(case, expected):
    assert _.reg_exp_replace(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foo",), ""),
        (("foo", 0), ""),
        (("foo", 1), "foo"),
        (("foo", 3), "foofoofoo"),
        (("", 3), ""),
        (("", 0), ""),
        ((None, 0), ""),
        ((None, 1), ""),
    ],
)
def test_repeat(case, expected):
    assert _.repeat(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foo", "o", "a"), "faa"),
        (("foo", "o", "a", False, 1), "fao"),
        (("fOO", "o", "a"), "fOO"),
        (("fOO", "o", "a", True), "faa"),
        (("", "", ""), ""),
        (("foo", "o", ""), "f"),
        (("foo", "x", "y"), "foo"),
        (("foo", "", ""), "foo"),
        (("foo", "", None), "foo"),
        (("foo", None, None), "foo"),
        (("foo", None, ""), "foo"),
        (("foo", None, "a"), "foo"),
        ((54.7, 5, 6), "64.7"),
    ],
)
def test_replace(case, expected):
    assert _.replace(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foo", "o", "a"), "foa"),
        (("foo", "f", "a"), "foo"),
    ],
)
def test_replace_end(case, expected):
    assert _.replace_end(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foo", "o", "a"), "foo"),
        (("foo", "f", "a"), "aoo"),
    ],
)
def test_replace_start(case, expected):
    assert _.replace_start(*case) == expected


@parametrize(
    "case,expected",
    [
        (("foo  bar baz", "-"), "foo-bar-baz"),
        (("foo__bar_baz", "-"), "foo-bar-baz"),
        (("foo-_bar-_-baz", "-"), "foo-bar-baz"),
        (("foo!bar,baz", "-"), "foo-bar-baz"),
        (("--foo.bar;baz", "-"), "foo-bar-baz"),
        (("Foo Bar", "-"), "foo-bar"),
        (("foo  bar baz", "_"), "foo_bar_baz"),
        (("foo__bar_baz", "_"), "foo_bar_baz"),
        (("foo-_bar-_-baz", "_"), "foo_bar_baz"),
        (("foo!bar,baz", "_"), "foo_bar_baz"),
        (("--foo.bar;baz", "_"), "foo_bar_baz"),
        (("Foo Bar", "_"), "foo_bar"),
        (("", "_"), ""),
        ((None, "_"), ""),
    ],
)
def test_separator_case(case, expected):
    assert _.separator_case(*case) == expected


@parametrize(
    "case,expected",
    [
        (([],), ""),
        ((tuple(),), ""),
        (((None,),), ""),
        (((None, None),), ""),
        ((("", None),), ""),
        (((None, ""),), ""),
        (((None, 5),), "5"),
        (((7.88, None),), "7.88"),
        ((["", ""],), ""),
        ((["foo"],), "foo"),
        ((["foo", "bar"],), "foo and bar"),
        ((["foo", "bar", "baz"],), "foo, bar and baz"),
        ((["foo", "bar", "baz", "qux"], ", ", " or "), "foo, bar, baz or qux"),
        ((["foo", "bar", "baz", "qux"], ";", " or "), "foo;bar;baz or qux"),
        ((["foo", "bar", "baz", "qux"], 0.6, " or "), "foo0.6bar0.6baz or qux"),
        ((["foo", "bar", "baz", "qux"], 0.6, 1), "foo0.6bar0.6baz1qux"),
        ((["foo", "bar", "baz", "qux"], 0.6, None), "foo0.6bar0.6bazqux"),
        ((["foo", 23, "baz", "qux"], None, None), "foo23bazqux"),
    ],
)
def test_series_phrase(case, expected):
    assert _.series_phrase(*case) == expected


@parametrize(
    "case,expected",
    [
        (([],), ""),
        ((tuple(),), ""),
        (((None,),), ""),
        (((None, None),), ""),
        ((("", None),), ""),
        (((None, ""),), ""),
        (((None, 5),), "5"),
        (((7.88, None),), "7.88"),
        ((["", ""],), ""),
        ((["foo"],), "foo"),
        ((["foo", "bar"],), "foo and bar"),
        ((["foo", "bar", "baz"],), "foo, bar, and baz"),
        ((["foo", "bar", "baz", "qux"], ", ", " or "), "foo, bar, baz, or qux"),
        ((["foo", "bar", "baz", "qux"], ";", " or "), "foo;bar;baz; or qux"),
    ],
)
def test_series_phrase_serial(case, expected):
    assert _.series_phrase_serial(*case) == expected


@parametrize(
    "case,expected",
    [
        ("Foo Bar", "foo-bar"),
        (" foo bar ", "foo-bar"),
        (u"Un éléphant à l'orée du bois", "un-elephant-a-l-oree-du-bois"),
        ("", ""),
        (5, "5"),
        (None, ""),
    ],
)
def test_slugify(case, expected):
    assert _.slugify(case) == expected


@parametrize(
    "case,expected",
    [
        ("foo  bar baz", "foo_bar_baz"),
        ("foo__bar_baz", "foo_bar_baz"),
        ("foo-_bar-_-baz", "foo_bar_baz"),
        ("foo!bar,baz", "foo_bar_baz"),
        ("--foo.bar;baz", "foo_bar_baz"),
        ("FooBar", "foo_bar"),
        ("fooBar", "foo_bar"),
        ("", ""),
        (None, ""),
        (5, "5"),
    ],
)
def test_snake_case(case, expected):
    assert _.snake_case(case) == expected


@parametrize(
    "case,expected",
    [
        (("string1 string2",), ["string1", "string2"]),
        (("string", ""), ["s", "t", "r", "i", "n", "g"]),
        (("string", None), ["s", "t", "r", "i", "n", "g"]),
        (("", ""), []),
        (("", None), []),
        ((None, None), []),
        (("string1,string2", ","), ["string1", "string2"]),
    ],
)
def test_split(case, expected):
    assert _.split(*case) == expected


@parametrize(
    "case,expected",
    [
        ("foo bar baz", "Foo Bar Baz"),
        ("foo-bar-baz", "Foo Bar Baz"),
        ("Foo!#Bar's", "Foo Bar S"),
        ("fooBar", "Foo Bar"),
        ("___FOO-BAR___", "FOO BAR"),
        ("XMLHttp", "XML Http"),
        ("", ""),
        (None, ""),
        (5, "5"),
    ],
)
def test_start_case(case, expected):
    assert _.start_case(case) == expected


@parametrize(
    "case,expected",
    [
        (("abc", "a"), True),
        (("abc", "b"), False),
        (("abc", "a", 0), True),
        (("abc", "a", 1), False),
        (("abc", "b", 1), True),
        (("abc", "b", 2), False),
        ((5.78, 5), True),
        (("5.78", 5), True),
        ((5.78, "5"), True),
        ((5.78, ""), True),
        ((5.78, None), True),
        ((None, None), True),
        (("", None), True),
        ((" ", None), True),
    ],
)
def test_starts_with(case, expected):
    assert _.starts_with(*case) == expected


@parametrize(
    "case,expected",
    [
        ('a <a href="#">link</a>', "a link"),
        (
            'a <a href="#">link</a><script>alert("hello world!")</script>',
            'a linkalert("hello world!")',
        ),
        ("", ""),
        (None, ""),
    ],
)
def test_strip_tags(case, expected):
    assert _.strip_tags(case) == expected


@parametrize(
    "case,expected",
    [
        (("This_is_a_test_string", "_"), "This"),
        (("This_is_a_test_string", ""), "This_is_a_test_string"),
        (("This_is_a_test_string", " "), "This_is_a_test_string"),
        (("This_is_a_test_string", None), "This_is_a_test_string"),
        ((None, None), ""),
        ((None, "4"), ""),
        ((None, ""), ""),
    ],
)
def test_substr_left(case, expected):
    assert _.substr_left(*case) == expected


@parametrize(
    "case,expected",
    [
        (("This_is_a_test_string", "_"), "This_is_a_test"),
        (("This_is_a_test_string", ""), "This_is_a_test_string"),
        (("This_is_a_test_string", " "), "This_is_a_test_string"),
        (("This_is_a_test_string", None), "This_is_a_test_string"),
        ((None, None), ""),
        ((None, "4"), ""),
        ((None, ""), ""),
    ],
)
def test_substr_left_end(case, expected):
    assert _.substr_left_end(*case) == expected


@parametrize(
    "case,expected",
    [
        (("This_is_a_test_string", "_"), "is_a_test_string"),
        (("This_is_a_test_string", ""), "This_is_a_test_string"),
        (("This_is_a_test_string", " "), "This_is_a_test_string"),
        (("This_is_a_test_string", None), "This_is_a_test_string"),
        ((None, None), ""),
        ((None, "4"), ""),
        ((None, ""), ""),
    ],
)
def test_substr_right(case, expected):
    assert _.substr_right(*case) == expected


@parametrize(
    "case,expected",
    [
        (("This_is_a_test_string", "_"), "string"),
        (("This_is_a_test_string", ""), "This_is_a_test_string"),
        (("This_is_a_test_string", " "), "This_is_a_test_string"),
        (("This_is_a_test_string", None), "This_is_a_test_string"),
        ((None, None), ""),
        ((None, "4"), ""),
        ((None, ""), ""),
    ],
)
def test_substr_right_end(case, expected):
    assert _.substr_right_end(*case) == expected


@parametrize(
    "case,expected",
    [
        ("a", "b"),
        ("A", "B"),
    ],
)
def test_successor(case, expected):
    assert _.successor(case) == expected


@parametrize(
    "source,wrapper,expected",
    [
        ("hello world!", "*", "*hello world!*"),
        ("hello world!", "**", "**hello world!**"),
        ("", "**", "****"),
        ("hello world!", "", "hello world!"),
        (5, "1", "151"),
        (5, 1, "151"),
        ("5", 1, "151"),
        ("5", 12, "12512"),
        (5, "", "5"),
        ("", 5, "55"),
        ("", None, ""),
        (None, None, ""),
        (5, None, "5"),
    ],
)
def test_surround(source, wrapper, expected):
    assert _.surround(source, wrapper) == expected


@parametrize(
    "case,expected",
    [
        ("fOoBaR", "FoObAr"),
        ("", ""),
        (None, ""),
    ],
)
def test_swap_case(case, expected):
    assert _.swap_case(case) == expected


@parametrize(
    "case,expected",
    [
        ("foo bar baz", "Foo Bar Baz"),
        ("they're bill's friends from the UK", "They're Bill's Friends From The Uk"),
        ("", ""),
        (None, ""),
        (5, "5"),
    ],
)
def test_title_case(case, expected):
    assert _.title_case(case) == expected


@parametrize(
    "case,expected",
    [("--Foo-Bar--", "--foo-bar--"), ("fooBar", "foobar"), ("__FOO_BAR__", "__foo_bar__")],
)
def test_to_lower(case, expected):
    assert _.to_lower(case) == expected


@parametrize(
    "case,expected",
    [("--Foo-Bar--", "--FOO-BAR--"), ("fooBar", "FOOBAR"), ("__FOO_BAR__", "__FOO_BAR__")],
)
def test_to_upper(case, expected):
    assert _.to_upper(case) == expected


@parametrize(
    "case,expected",
    [
        (("  fred  ",), "fred"),
        (("-_-fred-_-", "_-"), "fred"),
        (("-_-fred-_-", None), "-_-fred-_-"),
        ((None,), ""),
        ((None, None), ""),
    ],
)
def test_trim(case, expected):
    assert _.trim(*case) == expected


@parametrize(
    "case,expected",
    [
        (("  fred  ",), "fred  "),
        (("-_-fred-_-", "_-"), "fred-_-"),
        (("-_-fred-_-", None), "-_-fred-_-"),
        ((None,), ""),
        ((None, None), ""),
    ],
)
def test_trim_start(case, expected):
    assert _.trim_start(*case) == expected


@parametrize(
    "case,expected",
    [
        (("  fred  ",), "  fred"),
        (("-_-fred-_-", "_-"), "-_-fred"),
        (("-_-fred-_-", None), "-_-fred-_-"),
        ((None,), ""),
        ((None, None), ""),
    ],
)
def test_trim_end(case, expected):
    assert _.trim_end(*case) == expected


@parametrize(
    "case,expected",
    [
        (("hi-diddly-ho there, neighborino",), "hi-diddly-ho there, neighbo..."),
        (("hi-diddly-ho there, neighborino", 24), "hi-diddly-ho there, n..."),
        (("hi-diddly-ho there, neighborino", 24, "...", " "), "hi-diddly-ho there,..."),
        (
            ("hi-diddly-ho there, neighborino", 24, "...", re.compile(",? +")),
            "hi-diddly-ho there...",
        ),
        (("hi-diddly-ho there, neighborino", 30, " [...]"), "hi-diddly-ho there, neig [...]"),
        (("123456789", 9), "123456789"),
        (("123456789", 8), "12345..."),
        (("x",), "x"),
        ((" ",), " "),
        (("",), ""),
        ((None,), ""),
    ],
)
def test_truncate(case, expected):
    assert _.truncate(*case) == expected


@parametrize(
    "case,expected",
    [
        ("abc&lt;&gt; &amp;&quot;&#39;&#96;efg", "abc<> &\"'`efg"),
        ("", ""),
        (" ", " "),
        (None, ""),
    ],
)
def test_unescape(case, expected):
    assert _.unescape(case) == expected


@parametrize(
    "case,expected",
    [
        (('"foo"',), "foo"),
        (("'foo'", "'"), "foo"),
        (('"foo',), '"foo'),
        (('foo"',), 'foo"'),
        ((" ",), " "),
        (("",), ""),
        ((None,), ""),
    ],
)
def test_unquote(case, expected):
    assert _.unquote(*case) == expected


@parametrize(
    "case,expected",
    [  # noqa
        ("fooBar", "FOO BAR"),
        ("--foo-Bar--", "FOO BAR"),
        ("*Foo*B_a*R", "FOO B A R"),
        ("*Foo10_B*Ar", "FOO 10 B AR"),
        ('/?*Foo10/;"B*Ar', "FOO 10 B AR"),
        ('/?F@O__o10456?>.B?>";Ar', "F O O 10456 B AR"),
        ("FoO Bar", "FO O BAR"),
        ("F\n\\soO Bar", "F SO O BAR"),
        ("Foo1054665Bar", "FOO 1054665 BAR"),
    ],
)
def test_upper_case(case, expected):
    assert _.upper_case(case) == expected


@parametrize(
    "case,expected",
    [("foobar", "Foobar"), ("Foobar", "Foobar"), ("1foobar", "1foobar"), (";foobar", ";foobar")],
)
def test_upper_first(case, expected):
    assert _.upper_first(case) == expected


@parametrize(
    "case,expected",
    [
        ({"args": [""], "kargs": {}}, ""),
        ({"args": ["/"], "kargs": {}}, "/"),
        ({"args": ["http://github.com"], "kargs": {}}, "http://github.com"),
        ({"args": ["http://github.com:80"], "kargs": {}}, "http://github.com:80"),
        (
            {"args": ["http://github.com:80", "pydash", "issues/"], "kargs": {}},
            "http://github.com:80/pydash/issues/",
        ),
        ({"args": ["/foo", "bar", "/baz", "/qux/"], "kargs": {}}, "/foo/bar/baz/qux/"),
        ({"args": ["/foo/bar"], "kargs": {"a": 1, "b": "two"}}, "/foo/bar?a=1&b=two"),
        ({"args": ["/foo/bar?x=5"], "kargs": {"a": 1, "b": "two"}}, "/foo/bar?x=5&a=1&b=two"),
        (
            {"args": ["/foo/bar?x=5", "baz?z=3"], "kargs": {"a": 1, "b": "two"}},
            "/foo/bar/baz?x=5&a=1&b=two&z=3",
        ),
        (
            {"args": ["/foo/bar?x=5", "baz?z=3"], "kargs": {"a": [1, 2], "b": "two"}},
            "/foo/bar/baz?x=5&a=1&a=2&b=two&z=3",
        ),
        (
            {"args": ["/foo#bar", "baz"], "kargs": {"a": [1, 2], "b": "two"}},
            "/foo?a=1&a=2&b=two#bar/baz",
        ),
        (
            {"args": ["/foo", "baz#bar"], "kargs": {"a": [1, 2], "b": "two"}},
            "/foo/baz?a=1&a=2&b=two#bar",
        ),
    ],
)
def test_url(case, expected):
    result = _.url(*case["args"], **case["kargs"])

    r_scheme, r_netloc, r_path, r_query, r_fragment = urlsplit(result)
    e_scheme, e_netloc, e_path, e_query, e_fragment = urlsplit(expected)

    assert r_scheme == e_scheme
    assert r_netloc == e_netloc
    assert r_path == e_path
    assert set(parse_qsl(r_query)) == set(parse_qsl(e_query))
    assert r_fragment == e_fragment


@parametrize(
    "case,expected",
    [  # noqa
        ("hello world!", ["hello", "world"]),  # noqa
        ("hello_world", ["hello", "world"]),
        ("hello!@#$%^&*()_+{}|:\"<>?-=[]\\;\\,.'/world", ["hello", "world"]),
        ("hello 12345 world", ["hello", "12345", "world"]),
        ("enable 24h format", ["enable", "24", "h", "format"]),
        ("tooLegit2Quit", ["too", "Legit", "2", "Quit"]),
        ("xhr2Request", ["xhr", "2", "Request"]),
        (" ", []),
        ("", []),
        (None, []),
    ],
)
def test_words(case, expected):
    assert _.words(case) == expected


@parametrize(
    "case,expected",
    [
        ("enable 24h format", "enable24HFormat"),
    ],
)
def test_word_cycle(case, expected):
    actual = _.chain(case).camel_case().kebab_case().snake_case().start_case().camel_case().value()

    assert actual == expected
