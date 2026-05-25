from __future__ import annotations

from traitlets.utils.bunch import Bunch


def test_bunch():
    b = Bunch(x=5, y=10)
    assert "y" in b
    assert "x" in b
    assert b.x == 5
    b["a"] = "hi"
    assert b.a == "hi"


def test_bunch_dir():
    b = Bunch(x=5, y=10)
    assert "keys" in dir(b)
    assert "x" in dir(b)
    assert "z" not in dir(b)
    b.z = 15
    assert "z" in dir(b)
