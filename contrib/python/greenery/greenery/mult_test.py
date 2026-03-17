from __future__ import annotations

from .bound import INF, Bound
from .charclass import DIGIT, Charclass
from .multiplier import ONE, PLUS, QM, STAR, Multiplier
from .rxelems import Mult


def test_mult_equality() -> None:
    a = Mult(Charclass("a"), ONE)
    # pylint: disable=comparison-with-itself
    assert a == a
    assert a != Mult(Charclass("b"), ONE)
    assert a != Mult(Charclass("a"), QM)
    assert a != Mult(Charclass("a"), Multiplier(Bound(1), Bound(2)))


def test_mult_str() -> None:
    a = Charclass("a")
    assert str(Mult(a, ONE)) == "a"
    assert str(Mult(a, Multiplier(Bound(2), Bound(2)))) == "a{2}"
    assert str(Mult(a, Multiplier(Bound(3), Bound(3)))) == "a{3}"
    assert str(Mult(a, Multiplier(Bound(4), Bound(4)))) == "a{4}"
    assert str(Mult(a, Multiplier(Bound(5), Bound(5)))) == "a{5}"
    assert str(Mult(a, QM)) == "a?"
    assert str(Mult(a, STAR)) == "a*"
    assert str(Mult(a, PLUS)) == "a+"
    assert str(Mult(a, Multiplier(Bound(2), Bound(5)))) == "a{2,5}"
    assert str(Mult(a, Multiplier(Bound(2), INF))) == "a{2,}"

    assert str(Mult(DIGIT, ONE)) == "\\d"
    assert str(Mult(DIGIT, Multiplier(Bound(2), Bound(2)))) == "\\d{2}"
    assert str(Mult(DIGIT, Multiplier(Bound(3), Bound(3)))) == "\\d{3}"


def test_odd_bug() -> None:
    # pylint: disable=invalid-name

    # Odd bug with ([bc]*c)?[ab]*
    int5A = Mult(
        Charclass("bc"),
        STAR,
    ).to_fsm()
    assert int5A.accepts("")

    int5B = Mult(
        Charclass("c"),
        ONE,
    ).to_fsm()
    assert int5B.accepts("c")

    int5C = int5A.concatenate(int5B)
    assert int5C.accepts("c")


def test_mult_common() -> None:
    a = Charclass("a")
    assert Mult(a, Multiplier(Bound(3), Bound(4))).common(
        Mult(a, Multiplier(Bound(2), Bound(5)))
    ) == Mult(a, Multiplier(Bound(2), Bound(3)))
    assert Mult(a, Multiplier(Bound(2), INF)).common(
        Mult(a, Multiplier(Bound(1), Bound(5)))
    ) == Mult(a, Multiplier(Bound(1), Bound(5)))
    assert Mult(a, Multiplier(Bound(3), INF)).common(
        Mult(a, Multiplier(Bound(2), INF))
    ) == Mult(a, Multiplier(Bound(2), INF))


def test_mult_dock() -> None:
    a = Charclass("a")
    assert Mult(a, Multiplier(Bound(4), Bound(5))).dock(
        Mult(a, Multiplier(Bound(3), Bound(3)))
    ) == Mult(a, Multiplier(Bound(1), Bound(2)))
