from typing import Literal, Optional, Union, Type

MAX_HTML_INPUT_SIZE = 250e+7

ParserCls = Union[Type["HTMLParser"], Type["LexborHTMLParser"]]
Parser = Union["HTMLParser", "LexborHTMLParser"]
FRAGMENT = Literal[
    "document",
    "fragment",
    "head",
    "body",
    "head_and_body",
    "document_no_head",
    "document_no_body",
    "document_no_head_no_body",
]


def preprocess_input(html, decode_errors='ignore'):
    if isinstance(html, (str, unicode)):
        bytes_html = html.encode('UTF-8', errors=decode_errors)
    elif isinstance(html, bytes):
        bytes_html = html
    else:
        raise TypeError("Expected a string, but %s found" % type(html).__name__)
    html_len = len(bytes_html)
    if html_len > MAX_HTML_INPUT_SIZE:
        raise ValueError("The specified HTML input is too large to be processed (%d bytes)" % html_len)
    return bytes_html, html_len


def do_create_tag(tag: str, parser_cls: ParserCls):
    if not tag:
        raise ValueError("Tag name cannot be empty")
    return do_parse_fragment(f"<{tag}></{tag}>", parser_cls)[0]


def get_fragment_type(
    html: str,
    parser_cls: ParserCls,
    tree: Optional[Parser] = None,
) -> FRAGMENT:
    if not tree:
        tree = parser_cls(html)

    import re
    html_re = re.compile(r"<html|<body|<head(?!er)", re.IGNORECASE)

    has_html = False
    has_head = False
    has_body = False
    for match in html_re.finditer(html):
        if match[0] == "<html":
            has_html = True
        elif match[0] == "<head":
            has_head = True
        elif match[0] == "<body":
            has_body = True

        if has_html and has_head and has_body:
            break

    if has_html and has_head and has_body:
        return "document"
    elif has_html and not has_head and has_body:
        return "document_no_head"
    elif has_html and has_head and not has_body:
        return "document_no_body"
    elif has_html and not has_head and not has_body:
        return "document_no_head_no_body"
    elif has_head and not has_body:
        return "head"
    elif not has_head and has_body:
        return "body"
    elif has_head and has_body:
        return "head_and_body"
    else:
        return "fragment"


def do_parse_fragment(html: str, parser_cls: ParserCls):
    """
    Given HTML, parse it into a list of Nodes, such that the nodes
    correspond to the given HTML.

    For contrast, HTMLParser adds `<html>`, `<head>`, and `<body>` tags
    if they are missing. This function does not add these tags.
    """
    html = html.strip()
    tree = parser_cls(html)
    frag_type = get_fragment_type(html, parser_cls, tree)

    if frag_type == "document":
        return [tree.root]
    if frag_type == "document_no_head":
        tree.head.decompose(recursive=True)
        return [tree.root]
    if frag_type == "document_no_body":
        tree.body.decompose(recursive=True)
        return [tree.root]
    if frag_type == "document_no_head_no_body":
        tree.head.decompose(recursive=True)
        tree.body.decompose(recursive=True)
        return [tree.root]
    elif frag_type == "head":
        tree.body.decompose(recursive=True)
        return [tree.head]
    elif frag_type == "body":
        tree.head.decompose(recursive=True)
        return [tree.body]
    elif frag_type == "head_and_body":
        return [tree.head, tree.body]
    else:
        return [
            *tree.head.iter(include_text=True),
            *tree.body.iter(include_text=True),
        ]
