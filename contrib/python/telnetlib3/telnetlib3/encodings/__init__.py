"""
Custom BBS/retro-computing codecs for telnetlib3.

Registers atascii, petscii, and atarist codecs with Python's codecs module
on import.  These encodings are then available for use with
``bytes.decode()`` and the ``--encoding`` CLI flag of
``telnetlib3-fingerprint``.
"""

# std imports
import codecs
import importlib
from typing import Optional

_cache: dict[str, Optional[codecs.CodecInfo]] = {}
_aliases: dict[str, codecs.CodecInfo] = {}


def _search_function(encoding: str) -> Optional[codecs.CodecInfo]:
    """Codec search function registered with codecs.register()."""
    normalized = encoding.lower().replace("-", "_")

    if normalized in _aliases:
        return _aliases[normalized]

    if normalized in _cache:
        return _cache[normalized]

    try:
        mod = importlib.import_module(f".{normalized}", package=__name__)
    except ImportError:
        _cache[normalized] = None
        return None

    try:
        info: codecs.CodecInfo = mod.getregentry()
    except AttributeError:
        _cache[normalized] = None
        return None

    _cache[normalized] = info

    if hasattr(mod, "getaliases"):
        for alias in mod.getaliases():
            _aliases[alias] = info

    return info


#: Retro BBS encoding names (and aliases) that additionally require raw mode.
#: Used by the client CLI entry point to auto-enable ``--raw-mode``.
FORCE_BINARY_ENCODINGS = frozenset(
    {
        "atascii",
        "atari8bit",
        "atari_8bit",
        "petscii",
        "cbm",
        "commodore",
        "c64",
        "c128",
        "atarist",
        "atari",
    }
)

codecs.register(_search_function)
