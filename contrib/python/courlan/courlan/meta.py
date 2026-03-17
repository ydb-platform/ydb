"""
Meta-functions to be applied module-wide.
"""

from urllib.parse import clear_cache as urllib_clear_cache  # type: ignore[attr-defined]

from .filters import langcodes_score


def clear_caches() -> None:
    """Reset all known LRU caches used to speed up processing.
    This may release some memory."""
    urllib_clear_cache()
    langcodes_score.cache_clear()
