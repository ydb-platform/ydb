"""Helper functions for string cleaning.

`normality` includes functions to convert arbitrary Python objects to
strings, transliterate them into the latin alphabet, make slugs for
URLs, or perform the substitution of characters based on unicode
character categories.
"""

from typing import Any, Optional

from normality.cleaning import collapse_spaces, squash_spaces, category_replace
from normality.constants import UNICODE_CATEGORIES, WS
from normality.transliteration import latinize_text, ascii_text
from normality.encoding import guess_encoding, guess_file_encoding
from normality.encoding import predict_encoding, predict_file_encoding
from normality.encoding import DEFAULT_ENCODING
from normality.stringify import stringify
from normality.paths import safe_filename
from normality.slugify import slugify, slugify_text
from normality.util import Categories, Encoding

__version__ = "3.0.2"
__all__ = [
    "collapse_spaces",
    "squash_spaces",
    "category_replace",
    "safe_filename",
    "normalize",
    "stringify",
    "slugify",
    "slugify_text",
    "guess_encoding",
    "guess_file_encoding",
    "predict_encoding",
    "predict_file_encoding",
    "latinize_text",
    "ascii_text",
    "WS",
    "UNICODE_CATEGORIES",
    "DEFAULT_ENCODING",
]


def normalize(
    value: Any,
    lowercase: bool = True,
    collapse: bool = True,
    latinize: bool = False,
    ascii: bool = False,
    encoding_default: Encoding = DEFAULT_ENCODING,
    encoding: Optional[str] = None,
    replace_categories: Categories = UNICODE_CATEGORIES,
) -> Optional[str]:
    """The main normalization function for text.

    This will take a string and apply a set of transformations to it so
    that it can be processed more easily afterwards. Arguments:

    * ``lowercase``: not very mysterious.
    * ``collapse``: replace multiple whitespace-like characters with a
      single whitespace. This is especially useful with category replacement
      which can lead to a lot of whitespace.
    * ``decompose``: apply a unicode normalization (NFKD) to separate
      simple characters and their diacritics.
    * ``replace_categories``: This will perform a replacement of whole
      classes of unicode characters (e.g. symbols, marks, numbers) with a
      given character. It is used to replace any non-text elements of the
      input string.
    """
    text = stringify(value, encoding_default=encoding_default, encoding=encoding)
    if text is None:
        return None

    if lowercase:
        # Yeah I made a Python package for this.
        text = text.lower()

    if ascii:
        # A stricter form of transliteration that leaves only ASCII
        # characters.
        text = ascii_text(text)
    elif latinize:
        # Perform unicode-based transliteration, e.g. of cyricllic
        # or CJK scripts into latin.
        text = latinize_text(text)

    # Perform unicode category-based character replacement. This is
    # used to filter out whole classes of characters, such as symbols,
    # punctuation, or whitespace-like characters.
    if replace_categories is not None:
        text = category_replace(text, replace_categories)

    if collapse:
        # Remove consecutive whitespace and strip
        text = squash_spaces(text)

    if len(text) == 0:
        return None
    return text
