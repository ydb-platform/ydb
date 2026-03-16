#
# Copyright (c) 2016 Fireeye, Inc. All rights reserved.
#
# Modifications Copyright (c) nexB, Inc. and others. All rights reserved.
#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
# This file contains code derived and modified from Fireeye's flare FLOSS:
# https://www.github.com/fireeye/flare-floss
# The original code was taken on 2016-10-28 from:
# https://raw.githubusercontent.com/fireeye/flare-floss/0db13aff88d0487f818f19a36de879dc27c94e13/floss/strings.py
# modifications:
# - do not return offsets
# - do not check for buffers filled with a single byte
# - removed main()
# - do not cache compiled patterns. re does cache patterns alright.


import re

ASCII_BYTE = (
    " !\"#\$%&\'\(\)\*\+,-\./0123456789:;<=>\?@ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "\[\]\^_`abcdefghijklmnopqrstuvwxyz\{\|\}\\\~\t"
)


def extract_ascii_strings(buf, n=3):
    """
    Yield Unicode strings (ASCII) from a buf string of binary data.

    :param buf: A bytestring.
    :type buf: str
    :param n: The minimum length of strings to extract.
    :type n: int
    :rtype: Sequence[string]
    """
    if not buf:
        return

    reg = '([%s]{%d,})' % (ASCII_BYTE, n)
    r = re.compile(reg)
    for match in r.finditer(buf):
        yield match.group().decode('ascii')


def extract_unicode_strings(buf, n=3):
    """
    Yield Unicode strings (UTF-16-LE encoded ASCII) from a buf string of binary data.

    :param buf: A bytestring.
    :type buf: str
    :param n: The minimum length of strings to extract.
    :type n: int
    :rtype: Sequence[string]
    """
    if not buf:
        return

    reg = b'((?:[%s]\x00){%d,})' % (ASCII_BYTE, n)
    r = re.compile(reg)
    for match in r.finditer(buf):
        try:
            yield match.group().decode('utf-16')
        except UnicodeDecodeError:
            pass


def extract_strings(buf, n=3):
    """
    Yield unicode strings (ASCII and UTF-16-LE encoded ASCII) from a buf string of binary data.

    :param buf: A bytestring.
    :type buf: str
    :param n: The minimum length of strings to extract.
    :type n: int
    :rtype: Sequence[string]
    """
    for s in extract_ascii_strings(buf):
        yield s

    for s in extract_unicode_strings(buf):
        yield s
