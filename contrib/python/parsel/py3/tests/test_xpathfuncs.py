from __future__ import annotations

from typing import Any

import pytest

from parsel import Selector
from parsel.xpathfuncs import set_xpathfunc


def test_has_class_simple() -> None:
    body = """
    <p class="foo bar-baz">First</p>
    <p class="foo">Second</p>
    <p class="bar">Third</p>
    <p>Fourth</p>
    """
    sel = Selector(text=body)
    assert [x.extract() for x in sel.xpath('//p[has-class("foo")]/text()')] == [
        "First",
        "Second",
    ]
    assert [x.extract() for x in sel.xpath('//p[has-class("bar")]/text()')] == ["Third"]
    assert [x.extract() for x in sel.xpath('//p[has-class("foo","bar")]/text()')] == []
    assert [
        x.extract() for x in sel.xpath('//p[has-class("foo","bar-baz")]/text()')
    ] == ["First"]


def test_has_class_error_no_args() -> None:
    body = """
    <p CLASS="foo">First</p>
    """
    sel = Selector(text=body)
    with pytest.raises(ValueError, match="has-class must have at least 1 argument"):
        sel.xpath("has-class()")


def test_has_class_error_invalid_arg_type() -> None:
    body = """
    <p CLASS="foo">First</p>
    """
    sel = Selector(text=body)
    with pytest.raises(ValueError, match="has-class arguments must be strings"):
        sel.xpath("has-class(.)")


def test_has_class_error_invalid_unicode() -> None:
    body = """
    <p CLASS="foo">First</p>
    """
    sel = Selector(text=body)
    with pytest.raises(ValueError, match="All strings must be XML compatible"):
        sel.xpath('has-class("héllö")'.encode())  # type: ignore[arg-type]


def test_has_class_unicode() -> None:
    body = """
    <p CLASS="fóó">First</p>
    """
    sel = Selector(text=body)
    assert [x.extract() for x in sel.xpath('//p[has-class("fóó")]/text()')] == ["First"]


def test_has_class_uppercase() -> None:
    body = """
    <p CLASS="foo">First</p>
    """
    sel = Selector(text=body)
    assert [x.extract() for x in sel.xpath('//p[has-class("foo")]/text()')] == ["First"]


def test_has_class_newline() -> None:
    body = """
    <p CLASS="foo
    bar">First</p>
    """
    sel = Selector(text=body)
    assert [x.extract() for x in sel.xpath('//p[has-class("foo")]/text()')] == ["First"]


def test_has_class_tab() -> None:
    body = """
    <p CLASS="foo\tbar">First</p>
    """
    sel = Selector(text=body)
    assert [x.extract() for x in sel.xpath('//p[has-class("foo")]/text()')] == ["First"]


def test_set_xpathfunc() -> None:
    def myfunc(ctx: Any) -> None:
        myfunc.call_count += 1  # type: ignore[attr-defined]

    myfunc.call_count = 0  # type: ignore[attr-defined]

    body = """
    <p CLASS="foo">First</p>
    """
    sel = Selector(text=body)
    with pytest.raises(ValueError, match=r"Unregistered function(: myfunc)? in myfunc"):
        sel.xpath("myfunc()")

    set_xpathfunc("myfunc", myfunc)
    sel.xpath("myfunc()")
    assert myfunc.call_count == 1  # type: ignore[attr-defined]

    set_xpathfunc("myfunc", None)
    with pytest.raises(ValueError, match=r"Unregistered function(: myfunc)? in myfunc"):
        sel.xpath("myfunc()")
