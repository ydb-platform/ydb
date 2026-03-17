# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

import re
import os
import sys
import importlib.resources
from importlib.resources.abc import TraversalError

MULTIPLE_WHITESPACE_PATTERN = re.compile(r"\s+", re.UNICODE)


def normalize_whitespace(text):
    """
    Translates multiple whitespace into single space character.
    If there is at least one new line character chunk is replaced
    by single LF (Unix new line) character.
    """
    return MULTIPLE_WHITESPACE_PATTERN.sub(_replace_whitespace, text)


def _replace_whitespace(match):
    """Normalize all spacing characters that aren't a newline to a space."""
    text = match.group()
    return "\n" if "\n" in text or "\r" in text else " "


def is_blank(string):
    """
    Returns `True` if string contains only white-space characters
    or is empty. Otherwise `False` is returned.
    """
    return not string or string.isspace()


def get_stoplists():
    """Returns a collection of built-in stop-lists."""
    stoplists_dir = importlib.resources.files(__package__) / "stoplists"

    stoplist_names = []
    for filename in stoplists_dir.iterdir():
        name, extension = os.path.splitext(filename.name)
        if extension == ".txt":
            stoplist_names.append(name)

    return frozenset(stoplist_names)


def get_stoplist(language):
    """Returns an built-in stop-list for the language as a set of words."""
    stoplists_dir = importlib.resources.files(__package__) / "stoplists"
    try:
        file_path = stoplists_dir / f"{language}.txt"
        stopwords = file_path.read_bytes()
    except (IOError, TraversalError):
        raise ValueError(
            "Stoplist for language '%s' is missing. "
            "Please use function 'get_stoplists' for complete list of stoplists "
            "and feel free to contribute by your own stoplist." % language
        )

    return frozenset(w.decode("utf8").lower() for w in stopwords.splitlines())
