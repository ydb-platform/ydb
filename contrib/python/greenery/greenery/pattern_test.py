from __future__ import annotations

from .charclass import Charclass
from .multiplier import ONE, ZERO
from .parse import parse
from .rxelems import Conc, Mult, Pattern


def test_pattern_equality() -> None:
    assert Pattern(
        Conc(Mult(Charclass("a"), ONE)),
        Conc(Mult(Charclass("b"), ONE)),
    ) == Pattern(
        Conc(Mult(Charclass("b"), ONE)),
        Conc(Mult(Charclass("a"), ONE)),
    )
    assert Pattern(
        Conc(Mult(Charclass("a"), ONE)),
        Conc(Mult(Charclass("a"), ONE)),
    ) == Pattern(
        Conc(Mult(Charclass("a"), ONE)),
    )


def test_pattern_str() -> None:
    assert (
        str(
            Pattern(
                Conc(Mult(Charclass("a"), ONE)),
                Conc(Mult(Charclass("b"), ONE)),
            )
        )
        == "a|b"
    )
    assert (
        str(
            Pattern(
                Conc(Mult(Charclass("a"), ONE)),
                Conc(Mult(Charclass("a"), ONE)),
            )
        )
        == "a"
    )
    assert (
        str(
            Pattern(
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
        )
        == "abc|def(ghi|jkl)"
    )


def test_empty() -> None:
    assert Pattern().empty()


def test_mult_reduction_easy() -> None:
    assert Pattern(Conc()).reduce() == Pattern(Conc())
    assert Pattern(
        Conc(
            Mult(
                Charclass("a"),
                ZERO,
            )
        )
    ).reduce() == Pattern(Conc())

    assert str(
        # pylint: disable-next=compare-to-empty-string
        Pattern(
            Conc(
                Mult(
                    Charclass("a"),
                    ZERO,
                )
            ).reduce()
        )
        == ""
    )


def test_empty_pattern_reduction() -> None:
    assert str(Pattern().reduce()) == "[]"


def test_empty_conc_suppression() -> None:
    assert (
        str(
            Pattern(
                Conc(
                    # this `Mult` can never actually match anything
                    Mult(Pattern(), ONE),
                    Mult(Charclass("0"), ONE),
                    Mult(Charclass("0123456789"), ONE),
                )  # so neither can this `Conc`
            ).reduce()
        )
        == "[]"
    )


def test_pattern_dock() -> None:
    a = Mult(Charclass("a"), ONE)
    c = Mult(Charclass("c"), ONE)
    f = Mult(Charclass("f"), ONE)

    assert parse("a|bc").dock(Conc()) == parse("a|bc")
    assert parse("aa|bca").dock(Conc(a)) == parse("a|bc")
    assert parse("xyza|abca|a").dock(Conc(a)) == parse("xyz|abc|")
    assert parse("f{2,3}c|fc").dock(Conc(f, c)) == parse("f{1,2}|")
    assert parse("aa").dock(Conc(a, a)) == parse("")


def test_pattern_beheading() -> None:
    a = Mult(Charclass("a"), ONE)
    c = Mult(Charclass("c"), ONE)
    f = Mult(Charclass("f"), ONE)
    z = Mult(Charclass("Z"), ONE)

    assert parse("aa").behead(Conc(a)) == parse("a")
    assert parse("abc|aa").behead(Conc(a)) == parse("a|bc")
    assert parse("cf{1,2}|cf").behead(Conc(c)) == parse("f{1,2}|f")
    assert parse("aa|aa").behead(Conc(a, a)) == parse("")
    assert parse("abc|aa").behead(Conc(a)) == parse("a|bc")
    assert parse("a|bc").behead(Conc()) == parse("a|bc")
    assert parse("cf{1,2}|cf").behead(Conc(c, f)) == parse("f?|")
    assert parse("ZA|ZB|ZC").behead(Conc(z)) == parse("A|B|C")
    assert parse("Z+A|ZB|ZZC").behead(Conc(z)) == parse("Z*A|B|ZC")
    assert parse("a{2}b|a+c").behead(Conc(a)) == parse("ab|a*c")
