"""
Transliterate the given text to the latin script.

This attempts to convert a given text to latin script using the
closest match of characters vis a vis the original script.

Transliteration requires an extensive unicode mapping. Since all
Python implementations are either GPL-licensed (and thus more
restrictive than this library) or come with a massive C code
dependency, this module requires neither but will use a package
if it is installed.
"""

from typing import Callable
from functools import lru_cache
import warnings

from icu import Transliterator  # type: ignore

Trans = Callable[[str], str]

# Transform to latin, separate accents, decompose, remove
# symbols, compose, push to ASCII
ASCII_SCRIPT = "Any-Latin; NFKD; [:Nonspacing Mark:] Remove; Accents-Any; [:Symbol:] Remove; [:Nonspacing Mark:] Remove; Latin-ASCII"  # noqa
# nb. 2021-11-05 Accents-Any is now followed with another nonspacing mark remover.
# This script is becoming a bit silly, there has to be a nicer way to do this?
_ASCII: Trans = Transliterator.createInstance(ASCII_SCRIPT).transliterate
_LATINIZE: Trans = Transliterator.createInstance("Any-Latin").transliterate

MAX_ASCII = 127  # No non-ASCII characters below this point.
MAX_LATIN = 740  # No non-latin characters below this point.


class ICUWarning(UnicodeWarning):
    pass


@lru_cache(maxsize=2**16)
def latinize_text(text: str, ascii: bool = False) -> str:
    """Transliterate the given text to the latin script.

    This attempts to convert a given text to latin script using the
    closest match of characters vis a vis the original script.
    """
    if text is None:
        warnings.warn(
            "normality.latinize_text will stop handling None soon.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ""

    if ascii:
        return ascii_text(text)

    is_latin = True
    for char in text:
        if ord(char) > MAX_LATIN:
            is_latin = False
            break
    if is_latin:
        # If the text is already latin, we can just return it.
        return text

    return _LATINIZE(text)


def ascii_text(text: str) -> str:
    """Transliterate the given text and make sure it ends up as ASCII."""
    if text is None:
        warnings.warn(
            "normality.ascii_text will stop handling None soon.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ""

    is_ascii = True
    for char in text:
        if ord(char) > MAX_ASCII:
            is_ascii = False
            break
    if is_ascii:
        # If the text is already ASCII, we can just return it.
        return text
    return _ascii_text(text)


@lru_cache(maxsize=2**16)
def _ascii_text(text: str) -> str:
    result = _ASCII(text)
    return result.encode("ascii", "replace").decode("ascii")
