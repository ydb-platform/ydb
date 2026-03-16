"""This module parses and generates contentlines as defined in RFC 5545
(iCalendar), but will probably work for other MIME types with similar syntax.
Eg. RFC 2426 (vCard)

It is stupid in the sense that it treats the content purely as strings. No type
conversion is attempted.
"""

from .content_line import Contentline, Contentlines
from .parameter import (
    Parameters,
    dquote,
    param_value,
    q_join,
    q_split,
    rfc_6868_escape,
    rfc_6868_unescape,
    validate_param_value,
)
from .property import (
    split_on_unescaped_comma,
    split_on_unescaped_semicolon,
    unescape_backslash,
    unescape_list_or_string,
)
from .string import (
    escape_char,
    escape_string,
    foldline,
    unescape_char,
    unescape_string,
    validate_token,
)

__all__ = [
    "Contentline",
    "Contentlines",
    "Parameters",
    "dquote",
    "escape_char",
    "escape_string",
    "foldline",
    "param_value",
    "q_join",
    "q_split",
    "rfc_6868_escape",
    "rfc_6868_unescape",
    "split_on_unescaped_comma",
    "split_on_unescaped_semicolon",
    "unescape_backslash",
    "unescape_char",
    "unescape_list_or_string",
    "unescape_string",
    "validate_param_value",
    "validate_token",
]
