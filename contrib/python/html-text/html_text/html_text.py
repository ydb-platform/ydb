from __future__ import annotations

import re
from typing import TYPE_CHECKING

import lxml
import lxml.etree
from lxml.html.clean import Cleaner

if TYPE_CHECKING:
    from collections.abc import Iterable

    import parsel

NEWLINE_TAGS: frozenset[str] = frozenset(
    [
        "article",
        "aside",
        "br",
        "dd",
        "details",
        "div",
        "dt",
        "fieldset",
        "figcaption",
        "footer",
        "form",
        "header",
        "hr",
        "legend",
        "li",
        "main",
        "nav",
        "table",
        "tr",
    ]
)
DOUBLE_NEWLINE_TAGS: frozenset[str] = frozenset(
    [
        "blockquote",
        "dl",
        "figure",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "ol",
        "p",
        "pre",
        "title",
        "ul",
    ]
)

cleaner = Cleaner(
    scripts=True,
    javascript=False,  # onclick attributes are fine
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=False,  # <title> may be nice to have
    processing_instructions=True,
    embedded=True,
    frames=True,
    forms=False,  # keep forms
    annoying_tags=False,
    remove_unknown_tags=False,
    safe_attrs_only=False,
)


def _cleaned_html_tree(html: lxml.html.HtmlElement | str) -> lxml.html.HtmlElement:
    tree = html if isinstance(html, lxml.html.HtmlElement) else parse_html(html)

    # we need this as https://bugs.launchpad.net/lxml/+bug/1838497
    try:
        cleaned = cleaner.clean_html(tree)
    except AssertionError:
        cleaned = tree

    return cleaned


def parse_html(html: str) -> lxml.html.HtmlElement:
    """Create an lxml.html.HtmlElement from a string with html.
    XXX: mostly copy-pasted from parsel.selector.create_root_node
    """
    body = html.strip().replace("\x00", "").encode("utf-8") or b"<html/>"
    parser = lxml.html.HTMLParser(recover=True, encoding="utf-8")
    root = lxml.etree.fromstring(body, parser=parser)
    if root is None:
        root = lxml.etree.fromstring(b"<html/>", parser=parser)
    return root


_whitespace = re.compile(r"\s+")
_has_trailing_whitespace = re.compile(r"\s$").search
_has_punct_after = re.compile(r'^[,:;.!?")]').search
_has_open_bracket_before = re.compile(r"\($").search


def _normalize_whitespace(text: str) -> str:
    return _whitespace.sub(" ", text.strip())


def etree_to_text(
    tree: lxml.html.HtmlElement,
    guess_punct_space: bool = True,
    guess_layout: bool = True,
    newline_tags: Iterable[str] = NEWLINE_TAGS,
    double_newline_tags: Iterable[str] = DOUBLE_NEWLINE_TAGS,
) -> str:
    """
    Convert a html tree to text. Tree should be cleaned with
    ``html_text.html_text.cleaner.clean_html`` before passing to this
    function.

    See html_text.extract_text docstring for description of the
    approach and options.
    """
    chunks = []

    _NEWLINE = object()
    _DOUBLE_NEWLINE = object()
    prev = _DOUBLE_NEWLINE  # _NEWLINE, _DOUBLE_NEWLINE or content of the previous chunk (str)

    def should_add_space(text: str) -> bool:
        """Return True if extra whitespace should be added before text"""
        if prev in {_NEWLINE, _DOUBLE_NEWLINE}:
            return False
        if not guess_punct_space:
            return True
        assert isinstance(prev, str)
        return bool(
            _has_trailing_whitespace(prev)
            or (not _has_punct_after(text) and not _has_open_bracket_before(prev))
        )

    def get_space_between(text: str) -> str:
        if not text:
            return " "
        return " " if should_add_space(text) else ""

    def add_newlines(tag: str) -> None:
        nonlocal prev
        if not guess_layout:
            return
        if prev is _DOUBLE_NEWLINE:  # don't output more than 1 blank line
            return
        if tag in double_newline_tags:
            chunks.append("\n" if prev is _NEWLINE else "\n\n")
            prev = _DOUBLE_NEWLINE
        elif tag in newline_tags:
            if prev is not _NEWLINE:
                chunks.append("\n")
            prev = _NEWLINE

    def add_text(text_content: str | None) -> None:
        nonlocal prev
        text = _normalize_whitespace(text_content) if text_content else ""
        if not text:
            return
        space = get_space_between(text)
        chunks.extend([space, text])
        prev = text_content

    # Extract text from the ``tree``: fill ``chunks`` variable
    for event, el in lxml.etree.iterwalk(tree, events=("start", "end")):
        if event == "start":
            assert isinstance(el.tag, str)
            add_newlines(el.tag)
            add_text(el.text)
        elif event == "end":
            assert isinstance(el.tag, str)
            add_newlines(el.tag)
            if el is not tree:
                add_text(el.tail)

    return "".join(chunks).strip()


def selector_to_text(
    sel: parsel.Selector | parsel.SelectorList[parsel.Selector],
    guess_punct_space: bool = True,
    guess_layout: bool = True,
) -> str:
    """Convert a cleaned parsel.Selector to text.
    See html_text.extract_text docstring for description of the approach
    and options.
    """
    import parsel  # noqa: PLC0415

    if isinstance(sel, parsel.SelectorList):
        # if selecting a specific xpath
        text = []
        for s in sel:
            extracted = etree_to_text(
                s.root, guess_punct_space=guess_punct_space, guess_layout=guess_layout
            )
            if extracted:
                text.append(extracted)
        return " ".join(text)
    return etree_to_text(
        sel.root, guess_punct_space=guess_punct_space, guess_layout=guess_layout
    )


def cleaned_selector(html: lxml.html.HtmlElement | str) -> parsel.Selector:
    """Clean parsel.selector."""
    import parsel  # noqa: PLC0415

    try:
        tree = _cleaned_html_tree(html)
        sel = parsel.Selector(root=tree, type="html")
    except (
        lxml.etree.XMLSyntaxError,
        lxml.etree.ParseError,
        lxml.etree.ParserError,
        UnicodeEncodeError,
    ):
        # likely plain text
        assert isinstance(html, str)
        sel = parsel.Selector(html)
    return sel


def extract_text(
    html: lxml.html.HtmlElement | str | None,
    guess_punct_space: bool = True,
    guess_layout: bool = True,
    newline_tags: Iterable[str] = NEWLINE_TAGS,
    double_newline_tags: Iterable[str] = DOUBLE_NEWLINE_TAGS,
) -> str:
    """
    Convert html to text, cleaning invisible content such as styles.

    Almost the same as normalize-space xpath, but this also
    adds spaces between inline elements (like <span>) which are
    often used as block elements in html markup, and adds appropriate
    newlines to make output better formatted.

    html should be a unicode string or an already parsed lxml.html element.

    ``html_text.etree_to_text`` is a lower-level function which only accepts
    an already parsed lxml.html Element, and is not doing html cleaning itself.

    When guess_punct_space is True (default), no extra whitespace is added
    for punctuation. This has a slight (around 10%) performance overhead
    and is just a heuristic.

    When guess_layout is True (default), a newline is added
    before and after ``newline_tags`` and two newlines are added before
    and after ``double_newline_tags``. This heuristic makes the extracted
    text more similar to how it is rendered in the browser.

    Default newline and double newline tags can be found in
    `html_text.NEWLINE_TAGS` and `html_text.DOUBLE_NEWLINE_TAGS`.
    """
    if html is None:
        return ""
    no_content_nodes = (lxml.html.HtmlComment, lxml.html.HtmlProcessingInstruction)
    if isinstance(html, no_content_nodes):
        return ""
    cleaned = _cleaned_html_tree(html)
    return etree_to_text(
        cleaned,
        guess_punct_space=guess_punct_space,
        guess_layout=guess_layout,
        newline_tags=newline_tags,
        double_newline_tags=double_newline_tags,
    )
