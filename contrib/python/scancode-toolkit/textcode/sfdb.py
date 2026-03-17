#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

"""
Specialized processing for Spline Font Database files from Fontforge
https://fontforge.github.io/en-US/documentation/developers/sfdformat/
"""


def get_text_lines(location):
    """
    Return a list of unicode text lines extracted from a spline font DB file at
    `location`.
    """
    interesting_lines = (
        b'Copyright:', b'LangName:',
        b'FontName:', b'FullName:',
        b'FamilyName:', b'Version:',
    )
    with open(location, 'rb') as sfdb_file:
        for line in sfdb_file:
            if line.startswith(interesting_lines):
                yield line
