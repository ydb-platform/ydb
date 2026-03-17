__version__ = "0.7.1"

from .html_text import (
    DOUBLE_NEWLINE_TAGS,
    NEWLINE_TAGS,
    cleaned_selector,
    cleaner,
    etree_to_text,
    extract_text,
    parse_html,
    selector_to_text,
)

__all__ = (
    "DOUBLE_NEWLINE_TAGS",
    "NEWLINE_TAGS",
    "cleaned_selector",
    "cleaner",
    "etree_to_text",
    "extract_text",
    "parse_html",
    "selector_to_text",
)
