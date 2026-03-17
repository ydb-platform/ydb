include "../utils.pxi"

import re


def create_tag(tag: str):
    """
    Given an HTML tag name, e.g. `"div"`, create a single empty node for that tag,
    e.g. `"<div></div>"`.

    Use `LexborHTMLParser().create_node(..)` if you need to create a node tied to a specific parser instance.
    """
    return LexborHTMLParser(f"<{tag}></{tag}>", is_fragment=True).root


def parse_fragment(html: str):
    """
    Given HTML, parse it into a list of Nodes, such that the nodes
    correspond to the given HTML.

    For contrast, HTMLParser adds `<html>`, `<head>`, and `<body>` tags
    if they are missing. This function does not add these tags.
    """
    return do_parse_fragment(html, LexborHTMLParser)


def extract_html_comment(text: str) -> str:
    """Extract the inner content of an HTML comment string.

    Args:
        text: Raw HTML comment, including the ``<!--`` and ``-->`` markers.

    Returns:
        The comment body with surrounding whitespace stripped.

    Raises:
        ValueError: If the input is not a well-formed HTML comment.

    Examples:
        >>> extract_html_comment("<!-- hello -->")
        'hello'
    """
    if match := re.fullmatch(r"\s*<!--\s*(.*?)\s*-->\s*", text, flags=re.DOTALL):
        return match.group(1).strip()
    msg = "Input is not a valid HTML comment"
    raise ValueError(msg)


cdef inline bint is_empty_text_node(lxb_dom_node_t *text_node):
    """
    Check whether a node is a text node made up solely of HTML ASCII whitespace.

    Parameters
    ----------
    text_node : lxb_dom_node_t *
        Pointer to the node that should be inspected.

    Returns
    -------
    bint
        ``True`` if ``text_node`` is a text node whose character data contains
        only space, tab, newline, form feed, or carriage return characters;
        otherwise ``False``.
    """
    if text_node == NULL or text_node.type != LXB_DOM_NODE_TYPE_TEXT:
        return False

    cdef lxb_dom_character_data_t *text_character_data = <lxb_dom_character_data_t *> text_node
    cdef lexbor_str_t *text_buffer = &text_character_data.data
    cdef size_t text_length = text_buffer.length
    cdef lxb_char_t *text_bytes = text_buffer.data

    return _is_whitespace_only(text_bytes, text_length)


cdef inline bint _is_whitespace_only(const lxb_char_t *buffer, size_t buffer_length) nogil:
    """
    Determine whether a byte buffer consists only of HTML ASCII whitespace.

    Parameters
    ----------
    buffer : const lxb_char_t *
        Pointer to the buffer to inspect.
    buffer_length : size_t
        Number of bytes available in ``buffer``.

    Returns
    -------
    bint
        ``True`` if ``buffer`` is ``NULL``, empty, or contains only space
        (0x20), tab (0x09), line feed (0x0A), form feed (0x0C), or carriage
        return (0x0D) bytes; otherwise ``False``.

    Notes
    -----
    Mirrors Lexbor's ``lexbor_utils_whitespace`` macro and stays inline to
    keep the GIL released in hot loops.
    """
    cdef const lxb_char_t *cursor = buffer
    cdef const lxb_char_t *end = buffer + buffer_length
    cdef lxb_char_t current_char

    if buffer == NULL or buffer_length == 0:
        return True

    # Inline whitespace check mirroring lexbor_utils_whitespace(chr, !=, &&)
    while cursor < end:
        current_char = cursor[0]
        if (current_char != ' ' and current_char != '\t' and current_char != '\n'
                and current_char != '\f' and current_char != '\r'):
            return False
        cursor += 1

    return True
