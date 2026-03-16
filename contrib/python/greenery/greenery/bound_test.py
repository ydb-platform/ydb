from __future__ import annotations

import pytest

from .bound import INF, Bound


def test_ctor() -> None:
    assert Bound(None) == INF

    Bound(0)
    Bound(1)
    Bound(2)

    with pytest.raises(ValueError):
        Bound(-1)


def test_eq_neq() -> None:
    # pylint: disable=comparison-with-itself
    assert Bound(0) == Bound(0)
    assert INF == INF
    assert Bound(0) != Bound(1)
    assert Bound(0) != INF
    assert Bound(1) == Bound(1)
    assert Bound(None) == INF


def test_eq_neq_heterogeneous() -> None:
    assert Bound(1) != "blah"


def test_comparisons() -> None:
    # pylint: disable=comparison-with-itself
    # pylint: disable=unneeded-not

    assert Bound(0) < Bound(1)
    assert Bound(0) < INF
    assert Bound(1) < INF
    assert not INF < INF


def test_multiplication() -> None:
    assert Bound(0) * Bound(0) == Bound(0)
    assert Bound(0) * Bound(1) == Bound(0)
    assert Bound(0) * Bound(2) == Bound(0)
    assert Bound(0) * Bound(5) == Bound(0)
    assert Bound(0) * INF == Bound(0)

    assert Bound(1) * Bound(5) == Bound(5)
    assert Bound(2) * Bound(5) == Bound(10)
    assert Bound(0) * INF == Bound(0)
    assert Bound(2) * INF == INF
    assert INF * INF == INF
    assert INF * Bound(0) == Bound(0)
    assert Bound(1) * Bound(0) == Bound(0)


def test_addition() -> None:
    assert Bound(0) + Bound(0) == Bound(0)
    assert Bound(0) + Bound(1) == Bound(1)
    assert Bound(0) + Bound(5) == Bound(5)
    assert Bound(0) + INF == INF

    assert Bound(1) + Bound(0) == Bound(1)
    assert Bound(1) + Bound(1) == Bound(2)
    assert Bound(1) + Bound(5) == Bound(6)
    assert Bound(1) + INF == INF

    assert INF + Bound(0) == INF
    assert INF + Bound(1) == INF
    assert INF + INF == INF


def test_subtraction() -> None:
    assert Bound(0) - Bound(0) == Bound(0)
    assert Bound(1) - Bound(0) == Bound(1)
    assert Bound(6) - Bound(4) == Bound(2)
    assert Bound(5) - Bound(5) == Bound(0)

    assert INF - Bound(0) == INF
    assert INF - Bound(1) == INF
    assert INF - Bound(1000) == INF
    assert INF - INF == Bound(0)

    with pytest.raises(ArithmeticError):
        _ = Bound(5) - Bound(6)

    with pytest.raises(ArithmeticError):
        _ = Bound(0) - Bound(1)

    with pytest.raises(ArithmeticError):
        _ = Bound(0) - INF

    with pytest.raises(ArithmeticError):
        _ = Bound(10) - INF


def test_copy() -> None:
    assert INF.copy() == INF

    b = Bound(6)
    assert b.copy() == b


def test_bound_str() -> None:
    assert str(Bound(2)) == "2"

    # pylint: disable-next=compare-to-empty-string
    assert str(INF) == ""


def test_bound() -> None:
    assert min(Bound(0), INF) == Bound(0)
    assert min(Bound(1), INF) == Bound(1)
