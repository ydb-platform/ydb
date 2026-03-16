from __future__ import annotations

import pytest

from .bound import INF, Bound
from .charclass import DIGIT, DOT, NULLCHARCLASS, Charclass
from .multiplier import ONE, PLUS, STAR, Multiplier

# noinspection PyProtectedMember
from .parse import NoMatch, match_charclass, match_mult, parse
from .rxelems import Conc, Mult, Pattern

if __name__ == "__main__":
    raise RuntimeError(
        "Test files can't be run directly. Use `python -m pytest greenery`"
    )


def test_charclass_matching() -> None:
    assert match_charclass("a", 0) == (Charclass("a"), 1)
    assert match_charclass("aa", 1) == (Charclass("a"), 2)
    assert match_charclass("a$", 1) == (Charclass("$"), 2)
    assert match_charclass(".", 0) == (DOT, 1)

    with pytest.raises(IndexError):
        match_charclass("[", 0)

    with pytest.raises(NoMatch):
        match_charclass("a", 1)

    assert match_charclass("[\\d]", 0) == (DIGIT, 4)


def test_negatives_inside_charclasses() -> None:
    assert match_charclass("[\\D]", 0) == (~DIGIT, 4)
    assert match_charclass("[a\\D]", 0) == (~DIGIT, 5)
    assert match_charclass("[a1\\D]", 0) == (~Charclass("023456789"), 6)
    assert match_charclass("[1a\\D]", 0) == (~Charclass("023456789"), 6)
    assert match_charclass("[1\\D]", 0) == (~Charclass("023456789"), 5)
    assert match_charclass("[\\Da]", 0) == (~DIGIT, 5)
    assert match_charclass("[\\D1]", 0) == (~Charclass("023456789"), 5)
    assert match_charclass("[\\D1a]", 0) == (~Charclass("023456789"), 6)
    assert match_charclass("[\\D\\d]", 0) == (DOT, 6)
    assert match_charclass("[\\D\\D]", 0) == (~DIGIT, 6)

    # "Either non-whitespace or a non-digit" matches _anything_.
    assert match_charclass("[\\S\\D]", 0) == (DOT, 6)
    assert match_charclass("[\\S \\D]", 0) == (DOT, 7)


def test_negated_negatives_inside_charclasses() -> None:
    assert match_charclass("[^\\D]", 0) == (DIGIT, 5)
    assert match_charclass("[^a\\D]", 0) == (DIGIT, 6)
    assert match_charclass("[^a1\\D]", 0) == (Charclass("023456789"), 7)
    assert match_charclass("[^1a\\D]", 0) == (Charclass("023456789"), 7)
    assert match_charclass("[^1\\D]", 0) == (Charclass("023456789"), 6)
    assert match_charclass("[^\\Da]", 0) == (DIGIT, 6)
    assert match_charclass("[^\\D1]", 0) == (Charclass("023456789"), 6)
    assert match_charclass("[^\\D1a]", 0) == (Charclass("023456789"), 7)
    assert match_charclass("[^\\D\\d]", 0) == (NULLCHARCLASS, 7)
    assert match_charclass("[^\\D\\D]", 0) == (DIGIT, 7)

    # "Anything but non-whitespace and non-digit" matches _nothing_.
    assert match_charclass("[^\\S\\D]", 0) == (NULLCHARCLASS, 7)
    assert match_charclass("[^\\S \\D]", 0) == (NULLCHARCLASS, 8)


def test_match_nightmare_charclass() -> None:
    assert match_charclass("[\t\n\r -\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]", 0) == (
        Charclass(
            (
                ("\t", "\t"),
                ("\n", "\n"),
                ("\r", "\r"),
                (" ", "\uD7FF"),
                ("\uE000", "\uFFFD"),
                ("\U00010000", "\U0010FFFF"),
            )
        ),
        14,
    )


def test_mult_matching() -> None:
    assert match_mult("abcde[^fg]*", 5) == (Mult(~Charclass("fg"), STAR), 11)
    assert match_mult("abcde[^fg]*h{5}[a-z]+", 11) == (
        Mult(Charclass("h"), Multiplier(Bound(5), Bound(5))),
        15,
    )
    assert match_mult("abcde[^fg]*h{5}[a-z]+T{1,}", 15) == (
        Mult(Charclass("abcdefghijklmnopqrstuvwxyz"), PLUS),
        21,
    )
    assert match_mult("abcde[^fg]*h{5}[a-z]+T{2,}", 21) == (
        Mult(Charclass("T"), Multiplier(Bound(2), INF)),
        26,
    )


def test_lazy_multipliers() -> None:
    assert match_mult("abcde[^fg]*?", 5) == (Mult(~Charclass("fg"), STAR), 12)
    assert match_mult("abcde[^fg]*?h{5}?[a-z]+", 12) == (
        Mult(Charclass("h"), Multiplier(Bound(5), Bound(5))),
        17,
    )
    assert match_mult("abcde[^fg]*?h{5}?[a-z]+?T{1,}", 17) == (
        Mult(Charclass("abcdefghijklmnopqrstuvwxyz"), PLUS),
        24,
    )
    assert match_mult("abcde[^fg]*?h{5}?[a-z]+?T{2,}?", 24) == (
        Mult(Charclass("T"), Multiplier(Bound(2), INF)),
        30,
    )


def test_charclass_ranges() -> None:
    # Should accept arbitrary ranges of characters in charclasses. No longer
    # limited to alphanumerics. (User beware...)
    assert parse("[z{|}~]") == parse("[z-~]")
    assert parse("[\\w:;<=>?@\\[\\\\\\]\\^`]") == parse("[0-z]")


def test_hex_escapes() -> None:
    # Should be able to parse e.g. "\\x40"
    assert parse("\\x00") == parse("\x00")
    assert parse("\\x40") == parse("@")
    assert parse("[\\x40]") == parse("[@]")
    assert parse("[\\x41-\\x5a]") == parse("[A-Z]")


def test_w_d_s() -> None:
    # Allow "\w", "\d" and "\s" in charclasses
    assert parse("\\w") == parse("[0-9A-Z_a-z]")
    assert parse("[\\w~]") == parse("[0-9A-Z_a-z~]")
    assert parse("[\\da]") == parse("[0123456789a]")
    assert parse("[\\s]") == parse("[\t\n\r\f\v ]")


def test_mult_parsing() -> None:
    assert parse("[a-g]+") == Pattern(Conc(Mult(Charclass("abcdefg"), PLUS)))
    assert parse("[a-g0-8$%]+") == Pattern(
        Conc(Mult(Charclass("abcdefg012345678$%"), PLUS))
    )
    assert parse("[a-g0-8$%\\^]+") == Pattern(
        Conc(Mult(Charclass("abcdefg012345678$%^"), PLUS))
    )


def test_lazy_mult_parsing() -> None:
    assert parse("[a-g]+?") == Pattern(Conc(Mult(Charclass("abcdefg"), PLUS)))


def test_conc_parsing() -> None:
    assert parse("abcde[^fg]*h{5}[a-z]+") == Pattern(
        Conc(
            Mult(Charclass("a"), ONE),
            Mult(Charclass("b"), ONE),
            Mult(Charclass("c"), ONE),
            Mult(Charclass("d"), ONE),
            Mult(Charclass("e"), ONE),
            Mult(~Charclass("fg"), STAR),
            Mult(Charclass("h"), Multiplier(Bound(5), Bound(5))),
            Mult(Charclass("abcdefghijklmnopqrstuvwxyz"), PLUS),
        )
    )
    assert parse("[bc]*[ab]*") == Pattern(
        Conc(
            Mult(Charclass("bc"), STAR),
            Mult(Charclass("ab"), STAR),
        )
    )
    assert parse("abc...") == Pattern(
        Conc(
            Mult(Charclass("a"), ONE),
            Mult(Charclass("b"), ONE),
            Mult(Charclass("c"), ONE),
            Mult(DOT, ONE),
            Mult(DOT, ONE),
            Mult(DOT, ONE),
        )
    )
    assert parse("\\d{4}-\\d{2}-\\d{2}") == Pattern(
        Conc(
            Mult(DIGIT, Multiplier(Bound(4), Bound(4))),
            Mult(Charclass("-"), ONE),
            Mult(DIGIT, Multiplier(Bound(2), Bound(2))),
            Mult(Charclass("-"), ONE),
            Mult(DIGIT, Multiplier(Bound(2), Bound(2))),
        )
    )


def test_pattern_parsing() -> None:
    assert parse("abc|def(ghi|jkl)") == Pattern(
        Conc(
            Mult(Charclass("a"), ONE),
            Mult(Charclass("b"), ONE),
            Mult(Charclass("c"), ONE),
        ),
        Conc(
            Mult(Charclass("d"), ONE),
            Mult(Charclass("e"), ONE),
            Mult(Charclass("f"), ONE),
            Mult(
                Pattern(
                    Conc(
                        Mult(Charclass("g"), ONE),
                        Mult(Charclass("h"), ONE),
                        Mult(Charclass("i"), ONE),
                    ),
                    Conc(
                        Mult(Charclass("j"), ONE),
                        Mult(Charclass("k"), ONE),
                        Mult(Charclass("l"), ONE),
                    ),
                ),
                ONE,
            ),
        ),
    )

    # Accept the "non-capturing group" syntax, "(?: ... )" but give it no
    # special significance
    assert parse("(?:)") == parse("()")
    assert parse("(?:abc|def)") == parse("(abc|def)")
    parse("(:abc)")  # should give no problems

    # Named groups
    assert parse("(?P<ng1>abc)") == parse("(abc)")


def test_nightmare_pattern() -> None:
    assert parse("[\t\n\r -\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]*") == Pattern(
        Conc(
            Mult(
                Charclass(
                    (
                        ("\t", "\t"),
                        ("\n", "\n"),
                        ("\r", "\r"),
                        (" ", "\uD7FF"),
                        ("\uE000", "\uFFFD"),
                        ("\U00010000", "\U0010FFFF"),
                    )
                ),
                STAR,
            )
        )
    )
