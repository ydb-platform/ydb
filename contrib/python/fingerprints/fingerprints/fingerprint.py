import logging
from typing import Optional
from normality import WS, squash_spaces, stringify

from fingerprints.types import replace_types
from fingerprints.cleanup import clean_entity_prefix, clean_name_ascii
from fingerprints.cleanup import clean_brackets

log = logging.getLogger(__name__)


def fingerprint(
    text: Optional[str], keep_order: bool = False, keep_brackets: bool = False
) -> Optional[str]:
    text = stringify(text)
    if text is None:
        return None

    # this needs to happen before the replacements
    text = text.lower()
    text = clean_entity_prefix(text)

    if not keep_brackets:
        text = clean_brackets(text)

    # Super hard-core string scrubbing
    text = clean_name_ascii(text)
    if text is None:
        return None
    text = replace_types(text)

    if keep_order:
        text = squash_spaces(text)
    else:
        # final manicure, based on openrefine algo
        parts = [p for p in text.split(WS) if len(p)]
        text = WS.join(sorted(set(parts)))

    if not len(text):
        return None

    return text
