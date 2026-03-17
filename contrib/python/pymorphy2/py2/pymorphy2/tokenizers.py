# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import re


GROUPING_SPACE_REGEX = re.compile(r'([^\w_-]|[+])', re.UNICODE)

def simple_word_tokenize(text, _split=GROUPING_SPACE_REGEX.split):
    """
    Split text into tokens. Don't split by a hyphen.
    Preserve punctuation, but not whitespaces.
    """
    return [t for t in _split(text) if t and not t.isspace()]
