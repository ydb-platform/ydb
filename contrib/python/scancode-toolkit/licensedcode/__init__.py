#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#


# FIXME: we should consider the length of two rules and two matches when considering MAX_DIST
# eventually this should be skipped early right during the matching too
# maximum distance between two matches to merge
MAX_DIST = 50


# minimum number of tokens a match should have to be considered as worthy keeping
MIN_MATCH_LENGTH = 4
MIN_MATCH_HIGH_LENGTH = 3


# rules smaller than this are treated as "small rules"
SMALL_RULE = 15

