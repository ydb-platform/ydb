import string
from typing import Any, Optional

from normality.cleaning import squash_spaces, category_replace
from normality.constants import SLUG_CATEGORIES, WS
from normality.transliteration import ascii_text
from normality.stringify import stringify

VALID_CHARS = string.ascii_lowercase + string.digits + WS


def slugify(value: Any, sep: str = "-") -> Optional[str]:
    """A simple slug generator. Slugs are pure ASCII lowercase strings
    that can be used in URLs an other places where a name has to be
    machine-safe.

    Consider using :func:`normality.slugify_text` instead, which avoids
    unnecessary stringification and is more efficient."""
    text = stringify(value)
    if text is None:
        return None
    return slugify_text(text, sep=sep)


def slugify_text(text: str, sep: str = "-") -> Optional[str]:
    """Slugify a text string. This will transliterate the text to ASCII,
    replace whitespace with the given separator, and remove all
    characters that are not alphanumeric or the separator."""
    text = text.lower().replace(sep, WS)
    # run this first because it'll give better results on special
    # characters.
    replaced = category_replace(text, SLUG_CATEGORIES)
    text = ascii_text(replaced)
    text = squash_spaces(text)
    text = "".join([c for c in text if c in VALID_CHARS])
    if len(text) == 0:
        return None
    return text.replace(WS, sep)
