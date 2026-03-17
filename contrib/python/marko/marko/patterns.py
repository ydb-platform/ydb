"""
Some regex patterns
"""

import functools
import re
import string
import unicodedata

tags = [
    "address",
    "article",
    "aside",
    "base",
    "basefont",
    "blockquote",
    "body",
    "caption",
    "center",
    "col",
    "colgroup",
    "dd",
    "details",
    "dialog",
    "dir",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "frame",
    "frameset",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "head",
    "header",
    "hr",
    "html",
    "iframe",
    "legend",
    "li",
    "link",
    "main",
    "menu",
    "menuitem",
    "meta",
    "nav",
    "noframes",
    "ol",
    "optgroup",
    "option",
    "p",
    "param",
    "section",
    "source",
    "summary",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "title",
    "tr",
    "track",
    "ul",
]
tag_name = r"[A-Za-z][A-Za-z0-9\-]*"
attribute = (
    r"\s+[A-Za-z:_][A-Za-z0-9\-_\.:]*"
    r'(?:\s*=\s*(?:[^\s"\'`=<>]+|\'[^\']*\'|"[^"]*"))?'
)
attribute_no_lf = (
    r"[^\n\S]+[A-Za-z:_][A-Za-z0-9\-_\.:]*"
    r'(?:[^\n\S]*=[^\n\S]*(?:[^\s"\'`=<>]+|\'[^\n\']*\'|"[^\n"]*"))?'
)

whitespace = re.compile(r"\s+", flags=re.UNICODE)
uri = r"[A-Za-z][A-Za-z\-.+]{1,31}:[^\s<>]*?"
email = (
    r"[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9]"
    r"(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9]"
    r"(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*"
)


@functools.lru_cache(maxsize=128)
def is_punctuation(ch: str) -> bool:
    if ch in string.punctuation:
        return True
    category = unicodedata.category(ch)
    return category.startswith("P") or category.startswith("S")
