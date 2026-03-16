#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import fnmatch
import os

from commoncode import fileutils
from commoncode import paths

TRACE = False
if TRACE:
    import logging
    import sys

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

"""
Match files and directories paths based on inclusion and exclusion glob-style
patterns.

For example, this can be used to skip files that match ignore patterns,
similar to a version control ignore files such as .gitignore.

The pattern syntax is the same as fnmatch(5) as implemented in Python.

Patterns are applied to a path this way:
 - Paths are converted to POSIX paths before matching.
 - Patterns are NOT case-sensitive.
 - Leading slashes are ignored.
 - If the pattern contains a /, then the whole path must be matched;
   otherwise, the pattern matches if any path segment matches.
 - When matched, a directory content is matched recursively.
   For instance, when using patterns for ignoring, a matched directory will
   be ignored with its file and sub-directories at full depth.
 - The order of patterns does not matter, except for exclusions vs. inclusions.
 - Exclusion patterns are prefixed with an exclamation mark (bang or !)
   meaning that matched paths by that pattern will be excluded. Exclusions
   have precedence of inclusions.
 - Patterns starting with # are comments and skipped. use [#] for a literal #.
 - to match paths relative to some root path, you must design your patterns
   and the paths to be tested accordingly. This module does not handles this.

Patterns may include glob wildcards such as:
 - ? : matches any single character.
 - * : matches 0 or more characters.
 - [seq] : matches any character in seq
 - [!seq] :matches any character not in seq
For a literal match, wrap the meta-characters in brackets. For example, '[?]'
matches the character '?'.
"""


def is_included(path, includes=None, excludes=None):
    """
    Return a True if `path` is included based on mapping of `includes` and
    `excludes` glob patterns. If the `path` is empty, return False.

    Matching is done based on the set of `includes` and `excludes` patterns maps
    of {fnmatch pattern: message}. If `includes` are provided they are tested
    first. The `excludes` are tested second if provided.

    The ordering of the includes and excludes items does not matter and if a map
    is empty, it is not used for matching.
    """
    if not path or not path.strip():
        return False

    if not includes and not excludes:
        return True

    includes = includes or {}
    includes = {k: v for k, v in includes.items() if k}
    excludes = excludes or {}
    excludes = {k: v for k, v in excludes.items() if k}

    if includes:
        included = get_matches(path, includes, all_matches=False)
        if TRACE:
            logger.debug('in_fileset: path: %(path)r included:%(included)r' % locals())
        if not included:
            return False

    if excludes:
        excluded = get_matches(path, excludes, all_matches=False)
        if TRACE:
            logger.debug('in_fileset: path: %(path)r excluded:%(excluded)r .' % locals())
        if excluded:
            return False

    return True


def get_matches(path, patterns, all_matches=False):
    """
    Return a list of values (which are values from the matched `patterns`
    mappint of {pattern: value or message} if `path` is matched by any of the
    pattern from the `patterns` map or an empty list.
    If `all_matches` is False, stop and return on the first matched pattern.
    """
    if not path or not patterns:
        return False

    path = fileutils.as_posixpath(path).lower()
    pathstripped = path.lstrip('/0')
    if not pathstripped:
        return False

    segments = paths.split(pathstripped)

    if TRACE:
        logger.debug('_match: path: %(path)r patterns:%(patterns)r.' % locals())

    matches = []
    if not isinstance(patterns, dict):
        assert isinstance(patterns, (list, tuple)), 'Invalid patterns: {}'.format(patterns)
        patterns = {p: p for p in patterns}

    for pat, value in patterns.items():
        if not pat or not pat.strip():
            continue

        value = value or ''
        pat = pat.lstrip('/').lower()
        is_plain = '/' not in pat

        if is_plain:
            if any(fnmatch.fnmatchcase(s, pat) for s in segments):
                matches.append(value)
                if not all_matches:
                    break
        elif (fnmatch.fnmatchcase(path, pat) or fnmatch.fnmatchcase(pathstripped, pat)):
            matches.append(value)
            if not all_matches:
                break
    if TRACE:
        logger.debug('_match: matches: %(matches)r' % locals())

    if not all_matches:
        if matches:
            return matches[0]
        else:
            return False
    return matches


def load(location):
    """
    Return a sequence of patterns from a file at location.
    """
    if not location:
        return tuple()
    fn = os.path.abspath(os.path.normpath(os.path.expanduser(location)))
    msg = ('File %(location)s does not exist or not a file.') % locals()
    assert (os.path.exists(fn) and os.path.isfile(fn)), msg
    mode = 'r'
    with open(fn, mode) as f:
        return [l.strip() for l in f if l and l.strip()]


def includes_excludes(patterns, message):
    """
    Return a dict of included patterns and a dict of excluded patterns from a
    sequence of `patterns` strings and a `message` setting the message as
    value in the returned mappings. Ignore pattern as comments if prefixed
    with #. Use an empty string is message is None.
    """
    message = message or ''
    BANG = '!'
    POUND = '#'
    included = {}
    excluded = {}
    if not patterns:
        return included, excluded

    for pat in patterns:
        pat = pat.strip()
        if not pat or pat.startswith(POUND):
            continue
        if pat.startswith(BANG):
            cpat = pat.lstrip(BANG)
            if cpat:
                excluded[cpat] = message
            continue
        else:
            included.add[pat] = message
    return included, excluded
