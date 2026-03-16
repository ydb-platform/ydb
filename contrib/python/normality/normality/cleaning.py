import re
import unicodedata
from typing import Any, Optional
import warnings

from normality.constants import UNICODE_CATEGORIES, CONTROL_CODES, WS
from normality.util import Categories, is_text

COLLAPSE_RE = re.compile(
    r"[\s\u2028\u2000-\u200a\u2029\u0a00\u1680\u202f\u205f\u3000\ufeff]+", re.U
)
COLLAPSE_REMOVE_RE = re.compile(r"[\u200b\u200c\u200d\ufeff\u00ad\u2060-\u2064]", re.U)
BOM_RE = re.compile("^\ufeff", re.U)
UNSAFE_RE = re.compile(
    r"^\ufeff|[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f\x80-\x9f\u200b\u200c\u200d\u200e\u200f\u061c\u2060-\u2064\u00ad\ufeff]"
)
UNSAFE_SPACES_RE = re.compile(
    r"[\u2000-\u200a\u2028\u2029\u0a00\u202f\u205f\u3000]", re.U
)
QUOTES_RE = re.compile(r'^["\'](.*)["\']$')


def decompose_nfkd(text: Any) -> Optional[str]:
    """Perform unicode compatibility decomposition.

    This will replace some non-standard value representations in unicode and
    normalise them, while also separating characters and their diacritics into
    two separate codepoints.
    """
    if not is_text(text):
        return None
    return unicodedata.normalize("NFKD", text)


def compose_nfc(text: Any) -> Optional[str]:
    """Perform unicode composition."""
    if not is_text(text):
        return None
    return unicodedata.normalize("NFC", text)


def compose_nfkc(text: Any) -> Optional[str]:
    """Perform unicode composition."""
    if not is_text(text):
        return None
    return unicodedata.normalize("NFKC", text)


def strip_quotes(text: str) -> Optional[str]:
    """Remove double or single quotes surrounding a string."""
    if not is_text(text):
        warnings.warn(
            "normality.strip_quotes will stop handling None soon.",
            DeprecationWarning,
            stacklevel=2,
        )
        return None
    return QUOTES_RE.sub("\\1", text)


def category_replace(text: str, replacements: Categories = UNICODE_CATEGORIES) -> str:
    """Remove characters from a string based on unicode classes.

    This is a method for removing non-text characters (such as punctuation,
    whitespace, marks and diacritics) from a piece of text by class, rather
    than specifying them individually.
    """
    text = unicodedata.normalize("NFKD", text)
    characters = []
    for character in text:
        cat = unicodedata.category(character)
        replacement = replacements.get(cat, character)
        if replacement is not None:
            characters.append(replacement)
    return "".join(characters)


def remove_control_chars(text: str) -> str:
    """Remove just the control codes from a piece of text."""
    return category_replace(text, replacements=CONTROL_CODES)


def remove_unsafe_chars(text: str) -> str:
    """Remove unsafe unicode characters from a piece of text."""
    if text is None:
        warnings.warn(
            "normality.remove_unsafe_chars will stop handling None soon.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ""
    text = UNSAFE_SPACES_RE.sub(WS, text)
    return UNSAFE_RE.sub("", text)


def remove_byte_order_mark(text: str) -> str:
    """Remove a BOM from the beginning of the text."""
    if text is None:
        warnings.warn(
            "normality.remove_byte_order_mark will stop handling None soon.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ""
    return BOM_RE.sub("", text)


def collapse_spaces(text: str) -> Optional[str]:
    """Remove newlines, tabs and multiple spaces with single spaces."""
    warnings.warn(
        "normality.collapse_spaces is deprecated, use normality.squash_spaces instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    # TODO: Remove in 3.1:
    if text is None:
        return None

    text = COLLAPSE_RE.sub(WS, text).strip(WS)
    if len(text) == 0:
        return None
    return text


def squash_spaces(text: str) -> str:
    """Remove all whitespace characters from a piece of text."""
    text = COLLAPSE_REMOVE_RE.sub("", text)
    return COLLAPSE_RE.sub(WS, text).strip(WS)
