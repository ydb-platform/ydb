from normality import squash_spaces

from fingerprints.cleanup import clean_name_ascii
from fingerprints.types.replacer import get_replacer, NormFunc


def replace_types(text: str) -> str:
    """Chomp down company types to a more convention form."""
    return get_replacer()(text)


def remove_types(text: str, clean: NormFunc = clean_name_ascii) -> str:
    """Remove company type names from a piece of text.

    WARNING: This converts to ASCII by default, pass in a different
    `clean` function if you need a different behaviour."""
    cleaned = clean(text)
    if cleaned is None:
        return ""
    removed = get_replacer(clean, remove=True)(cleaned)
    return squash_spaces(removed)
