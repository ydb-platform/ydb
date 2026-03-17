"""Backwards compatibility with Python 3.9."""

from sys import version_info

if version_info >= (3, 10):
    KW_ONLY = {"kw_only": True}
    SLOTS = {"slots": True}
else:
    KW_ONLY = {}
    SLOTS = {}
