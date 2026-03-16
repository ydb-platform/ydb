"""
Selector tests for cssselect backend
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import cssselect
import pytest
from cssselect.parser import SelectorSyntaxError
from cssselect.xpath import ExpressionError
from packaging.version import Version

from parsel import Selector, css2xpath
from parsel.csstranslator import GenericTranslator, HTMLTranslator, TranslatorProtocol

HTMLBODY = """
<html>
<body>
<div>
 <a id="name-anchor" name="foo"></a>
 <a id="tag-anchor" rel="tag" href="http://localhost/foo">link</a>
 <a id="nofollow-anchor" rel="nofollow" href="https://example.org"> link</a>
 <p id="paragraph">
   lorem ipsum text
   <b id="p-b">hi</b> <em id="p-em">there</em>
   <b id="p-b2">guy</b>
   <input type="checkbox" id="checkbox-unchecked" />
   <input type="checkbox" id="checkbox-disabled" disabled="" />
   <input type="text" id="text-checked" checked="checked" />
   <input type="hidden" />
   <input type="hidden" disabled="disabled" />
   <input type="checkbox" id="checkbox-checked" checked="checked" />
   <input type="checkbox" id="checkbox-disabled-checked"
          disabled="disabled" checked="checked" />
   <fieldset id="fieldset" disabled="disabled">
     <input type="checkbox" id="checkbox-fieldset-disabled" />
     <input type="hidden" />
   </fieldset>
 </p>
 <map name="dummymap">
   <area shape="circle" coords="200,250,25" href="foo.html" id="area-href" />
   <area shape="default" id="area-nohref" />
 </map>
</div>
<div class="cool-footer" id="foobar-div" foobar="ab bc cde">
    <span id="foobar-span">foo ter</span>
</div>
</body></html>
"""


class _TestTranslatorBase(ABC):
    @property
    @abstractmethod
    def tr_cls(self) -> type[TranslatorProtocol]:
        raise NotImplementedError

    def c2x(self, css: str) -> str:
        return self.tr_cls().css_to_xpath(css)

    @pytest.mark.parametrize(
        ("css", "xpath"),
        [
            ("::attr(name)", "descendant-or-self::*/@name"),
            ("a::attr(href)", "descendant-or-self::a/@href"),
            (
                "a ::attr(img)",
                "descendant-or-self::a/descendant-or-self::*/@img",
            ),
            ("a > ::attr(class)", "descendant-or-self::a/*/@class"),
        ],
    )
    def test_attr_function(self, css: str, xpath: str) -> None:
        assert self.c2x(css) == xpath, css

    @pytest.mark.parametrize(
        ("css", "exc"),
        [
            ("::attr(12)", ExpressionError),
            ("::attr(34test)", ExpressionError),
            ("::attr(@href)", SelectorSyntaxError),
        ],
    )
    def test_attr_function_exception(self, css: str, exc: type[Exception]) -> None:
        with pytest.raises(exc):
            self.c2x(css)

    @pytest.mark.parametrize(
        ("css", "xpath"),
        [
            ("::text", "descendant-or-self::text()"),
            ("p::text", "descendant-or-self::p/text()"),
            ("p ::text", "descendant-or-self::p/descendant-or-self::text()"),
            ("#id::text", "descendant-or-self::*[@id = 'id']/text()"),
            ("p#id::text", "descendant-or-self::p[@id = 'id']/text()"),
            (
                "p#id ::text",
                "descendant-or-self::p[@id = 'id']/descendant-or-self::text()",
            ),
            ("p#id > ::text", "descendant-or-self::p[@id = 'id']/*/text()"),
            (
                "p#id ~ ::text",
                "descendant-or-self::p[@id = 'id']/following-sibling::*/text()",
            ),
            ("a[href]::text", "descendant-or-self::a[@href]/text()"),
            (
                "a[href] ::text",
                "descendant-or-self::a[@href]/descendant-or-self::text()",
            ),
            (
                "p::text, a::text",
                "descendant-or-self::p/text() | descendant-or-self::a/text()",
            ),
        ],
    )
    def test_text_pseudo_element(self, css: str, xpath: str) -> None:
        assert self.c2x(css) == xpath, css

    @pytest.mark.parametrize(
        ("css", "exc"),
        [
            ("::attribute(12)", ExpressionError),
            ("::text()", ExpressionError),
            ("::attr(@href)", SelectorSyntaxError),
        ],
    )
    def test_pseudo_function_exception(self, css: str, exc: type[Exception]) -> None:
        with pytest.raises(exc):
            self.c2x(css)

    @pytest.mark.parametrize(
        ("css", "exc"),
        [
            ("::text-node", ExpressionError),
        ],
    )
    def test_unknown_pseudo_element(self, css: str, exc: type[Exception]) -> None:
        with pytest.raises(exc):
            self.c2x(css)

    @pytest.mark.parametrize(
        ("css", "exc"),
        [
            (":text", ExpressionError),
            (":attribute(name)", ExpressionError),
        ],
    )
    def test_unknown_pseudo_class(self, css: str, exc: type[Exception]) -> None:
        with pytest.raises(exc):
            self.c2x(css)


class TestHTMLTranslator(_TestTranslatorBase):
    tr_cls = HTMLTranslator


class TestGenericTranslator(_TestTranslatorBase):
    tr_cls = GenericTranslator


def test_css2xpath() -> None:
    expected_xpath = (
        "descendant-or-self::*[@class and contains("
        "concat(' ', normalize-space(@class), ' '), ' some-class ')]"
    )
    assert css2xpath(".some-class") == expected_xpath


class TestCSSSelector:
    sel = Selector(text=HTMLBODY)

    def x(self, *a: Any, **kw: Any) -> list[str]:
        return [v.strip() for v in self.sel.css(*a, **kw).extract() if v.strip()]

    def test_selector_simple(self) -> None:
        for x in self.sel.css("input"):
            assert isinstance(x, self.sel.__class__), x
        assert self.sel.css("input").extract() == [
            x.extract() for x in self.sel.css("input")
        ]

    def test_text_pseudo_element(self) -> None:
        assert self.x("#p-b2") == ['<b id="p-b2">guy</b>']
        assert self.x("#p-b2::text") == ["guy"]
        assert self.x("#p-b2 ::text") == ["guy"]
        assert self.x("#paragraph::text") == ["lorem ipsum text"]
        assert self.x("#paragraph ::text") == ["lorem ipsum text", "hi", "there", "guy"]
        assert self.x("p::text") == ["lorem ipsum text"]
        assert self.x("p ::text") == ["lorem ipsum text", "hi", "there", "guy"]

    def test_attribute_function(self) -> None:
        assert self.x("#p-b2::attr(id)") == ["p-b2"]
        assert self.x(".cool-footer::attr(class)") == ["cool-footer"]
        assert self.x(".cool-footer ::attr(id)") == ["foobar-div", "foobar-span"]
        assert self.x('map[name="dummymap"] ::attr(shape)') == ["circle", "default"]

    def test_nested_selector(self) -> None:
        assert self.sel.css("p").css("b::text").extract() == ["hi", "guy"]
        assert self.sel.css("div").css("area:last-child").extract() == [
            '<area shape="default" id="area-nohref">'
        ]

    @pytest.mark.xfail(
        Version(cssselect.__version__) < Version("1.2.0"),
        reason="Support added in cssselect 1.2.0",
    )
    def test_pseudoclass_has(self) -> None:
        assert self.x("p:has(b)::text") == ["lorem ipsum text"]


class TestCSSSelectorBytes(TestCSSSelector):
    sel = Selector(body=bytes(HTMLBODY, encoding="utf-8"))
