# -*- coding: utf-8 -*-
#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import Counter
import logging
import os
import re

from commoncode.text import as_unicode
from typecode import get_type

"""
Extract text from HTML, XML and related angular markup-like files.
"""

logger = logging.getLogger(__name__)

bin_dir = os.path.join(os.path.dirname(__file__), 'bin')

extensions = ('.html', '.htm', '.php', '.phps', '.jsp', '.jspx' , '.xml', '.pom',)


def is_markup(location):
    """
    Return True is the file at `location` is some kind of markup, such as HTML,
    XML, PHP, etc.
    """
    T = get_type(location)

    # do not care for small files
    if T.size < 64:
        return False

    if not T.is_text:
        return False

    if location.endswith(extensions):
        return True

    with open(location, 'rb') as f:
        start = as_unicode(f.read(1024))

    if start.startswith('<'):
        return True

    # count whitespaces
    no_spaces = ''.join(start.split())

    # count opening and closing tags_count
    counts = Counter(c for c in no_spaces if c in '<>')

    if not all(c in counts for c in '<>'):
        return False

    if not all(counts.values()):
        return False

    # ~ 5 percent of tag <> markers
    has_tags = sum(counts.values()) / len(no_spaces) > 0.05

    # check if we have some significant proportion of tag-like characters
    open_close = counts['>'] / counts['<']
    # ratio of open to close tags should approach 1: accept a 20% drift
    balanced = abs(1 - open_close) < .2
    return has_tags and balanced


def demarkup(location):
    """
    Return an iterator of unicode text lines for the file at `location` lightly
    stripping markup if the file is some kind of markup, such as HTML, XML, PHP,
    etc. The whitespaces are collapsed to one space.
    """
    from textcode.analysis import unicode_text_lines

    for line in unicode_text_lines(location):
        yield demarkup_text(line)


def demarkup_text(text):
    """
    Return text lightly stripped from markup. The whitespaces are collapsed to
    one space.
    """

    # keep the opening tag name of certain tags that contains these strings
    # note: <s> are from debian copyright files
    kept_tags = (
        'lic', 'copy', 'www', 'http', 'auth', 'contr', 'leg', 'inc', '@', 
        '<s>', '</s>', '169', 'a9'
        )

    # find start and closing tags or the first white space whichever comes first
    # or entities
    # this regex is such that ' '.join(tags.split(a))==a

    tags_ents = re.compile(r'(</?[^\s></]+(?:>|\s)?|&[^\s&]+;|href|[\'"]?\/\>)', re.IGNORECASE).split

    cleaned = []
    for token in tags_ents(text):
        if token.lower().startswith(('<', '&', 'href')) and not any(k in token.lower() for k in kept_tags):
            continue
        else:
            cleaned.append(token)
    return u' '.join(cleaned)
