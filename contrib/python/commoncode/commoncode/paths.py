#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import ntpath
import posixpath
import re

from os.path import commonprefix

from commoncode.text import as_unicode
from commoncode.text import toascii
from commoncode.fileutils import as_posixpath
from commoncode.fileutils import as_winpath
from commoncode.fileutils import is_posixpath

"""
Various path utilities such as common prefix and suffix functions, conversion
to OS-safe paths and to POSIX paths.
"""
#
# Build OS-portable and safer paths


def safe_path(path, posix=False, preserve_spaces=False):
    """
    Convert `path` to a safe and portable POSIX path usable on multiple OSes.
    The returned path is an ASCII-only byte string, resolved for relative
    segments and itself relative.

    The `path` is treated as a POSIX path if `posix` is True or as a Windows
    path with blackslash separators otherwise.

    If `preserve_spaces` is True, then the spaces in `path` will not be replaced.
    """
    # if the path is UTF, try to use unicode instead
    if not isinstance(path, str):
        path = as_unicode(path)

    path = path.strip()

    if not is_posixpath(path):
        path = as_winpath(path)
        posix = False

    path = resolve(path, posix)

    _pathmod, path_sep = path_handlers(path, posix)

    segments = [s.strip() for s in path.split(path_sep) if s.strip()]
    segments = [portable_filename(s, preserve_spaces=preserve_spaces) for s in segments]

    if not segments:
        return '_'

    # always return posix
    path = '/'.join(segments)
    return as_posixpath(path)


def path_handlers(path, posix=True):
    """
    Return a path module and path separator to use for handling (e.g. split and
    join) `path` using either POSIX or Windows conventions depending on the
    `path` content. Force usage of POSIX conventions if `posix` is True.
    """
    # determine if we use posix or windows path handling
    is_posix = is_posixpath(path)
    use_posix = posix or is_posix
    pathmod = use_posix and posixpath or ntpath
    path_sep = '/' if use_posix else '\\'
    return pathmod, path_sep


def resolve(path, posix=True):
    """
    Return a resolved relative POSIX path from `path` where extra slashes
    including leading and trailing slashes are removed, dot '.' and dotdot '..'
    path segments have been removed or resolved as possible. When a dotdot path
    segment cannot be further resolved and would be "escaping" from the provided
    path "tree", it is replaced by the string 'dotdot'.

    The `path` is treated as a POSIX path if `posix` is True (default) or as a
    Windows path with blackslash separators otherwise.
    """
    if not path:
        return '.'

    path = path.strip()
    if not path:
        return '.'

    if not is_posixpath(path):
        path = as_winpath(path)
        posix = False

    pathmod, path_sep = path_handlers(path, posix)

    path = path.strip(path_sep)
    segments = [s.strip() for s in path.split(path_sep) if s.strip()]

    # remove empty (// or ///) or blank (space only) or single dot segments
    segments = [s for s in segments if s and s != '.']

    path = path_sep.join(segments)

    # resolves . dot, .. dotdot
    path = pathmod.normpath(path)

    segments = path.split(path_sep)

    # remove empty or blank segments
    segments = [s.strip() for s in segments if s and s.strip()]

    # is this a windows absolute path? if yes strip the colon to make this relative
    if segments and len(segments[0]) == 2 and segments[0].endswith(':'):
        segments[0] = segments[0][:-1]

    # replace any remaining (usually leading) .. segment with a literal "dotdot"
    dotdot = 'dotdot'
    dd = '..'
    segments = [dotdot if s == dd else s for s in segments if s]
    if segments:
        path = path_sep.join(segments)
    else:
        path = '.'

    path = as_posixpath(path)

    return path


legal_punctuation = r"!\#$%&\(\)\+,\-\.;\=@\[\]_\{\}\~"
legal_spaces = r" "
legal_chars = r'A-Za-z0-9' + legal_punctuation
legal_chars_inc_spaces = legal_chars + legal_spaces
illegal_chars_re = r'[^' + legal_chars + r']'
illegal_chars_exc_spaces_re = r'[^' + legal_chars_inc_spaces + r']'
replace_illegal_chars = re.compile(illegal_chars_re).sub
replace_illegal_chars_exc_spaces = re.compile(illegal_chars_exc_spaces_re).sub


def portable_filename(filename, preserve_spaces=False):
    """
    Return a new name for `filename` that is portable across operating systems.

    In particular the returned file name is guaranteed to be:
    - a portable name on most OSses using a limited ASCII characters set including
      some limited punctuation.
    - a valid name on Linux, Windows and Mac.

    Unicode file names are transliterated to plain ASCII.

    See for more details:
    - http://www.opengroup.org/onlinepubs/007904975/basedefs/xbd_chap03.html
    - https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
    - http://www.boost.org/doc/libs/1_36_0/libs/filesystem/doc/portability_guide.htm

    Also inspired by Werkzeug:
    https://raw.githubusercontent.com/pallets/werkzeug/8c2d63ce247ba1345e1b9332a68ceff93b2c07ab/werkzeug/utils.py

    If `preserve_spaces` is True, then spaces in `filename` will not be replaced.
    """
    filename = toascii(filename, translit=True)

    if not filename:
        return '_'

    if preserve_spaces:
        filename = replace_illegal_chars_exc_spaces('_', filename)
    else:
        filename = replace_illegal_chars('_', filename)

    # these are illegal both upper and lowercase and with or without an extension
    # we insert an underscore after the base name.
    windows_illegal_names = set([
        'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
        'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9',
        'aux', 'con', 'nul', 'prn'
    ])

    basename, dot, extension = filename.partition('.')
    if basename.lower() in windows_illegal_names:
        filename = ''.join([basename, '_', dot, extension])

    # no name made only of dots.
    if set(filename) == set(['.']):
        filename = 'dot' * len(filename)

    # replaced any leading dotdot
    if filename != '..' and filename.startswith('..'):
        while filename.startswith('..'):
            filename = filename.replace('..', '__', 1)

    return filename

#
# paths comparisons, common prefix and suffix extraction
#


def common_prefix(s1, s2):
    """
    Return the common leading subsequence of two sequences and its length.
    """
    if not s1 or not s2:
        return None, 0
    common = commonprefix((s1, s2,))
    if common:
        return common, len(common)
    else:
        return None, 0


def common_suffix(s1, s2):
    """
    Return the common trailing subsequence between two sequences and its length.
    """
    if not s1 or not s2:
        return None, 0
    # revert the seqs and get a common prefix
    common, lgth = common_prefix(s1[::-1], s2[::-1])
    # revert again
    common = common[::-1] if common else common
    return common, lgth


def common_path_prefix(p1, p2):
    """
    Return the common leading path between two posix paths and the number of
    matched path segments.
    """
    return _common_path(p1, p2, common_func=common_prefix)


def common_path_suffix(p1, p2):
    """
    Return the common trailing path between two posix paths and the number of
    matched path segments.
    """
    return _common_path(p1, p2, common_func=common_suffix)


def split(p):
    """
    Split a posix path in a sequence of segments, ignoring leading and trailing
    slash. Return an empty sequence for an empty path and the root path /.
    """
    if not p:
        return []
    p = p.strip('/').split('/')
    return [] if p == [''] else p


def _common_path(p1, p2, common_func):
    """
    Return a common leading or trailing path brtween paths `p1` and `p2` and the
    common length in number of segments using the `common_func` path comparison
    function.
    """
    common, lgth = common_func(split(p1), split(p2))
    common = '/'.join(common) if common else None
    return common, lgth
