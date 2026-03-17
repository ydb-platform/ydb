#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import os
import posixpath
import re
import shutil
import sys

from os.path import dirname
from os.path import join
from os.path import exists

from commoncode.fileutils import as_posixpath
from commoncode.fileutils import create_dir
from commoncode.fileutils import file_name
from commoncode.fileutils import parent_directory
from commoncode.text import toascii
from commoncode.system import on_linux

logger = logging.getLogger(__name__)
TRACE = False
if TRACE:
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

# Suffix added to extracted target_dir paths
EXTRACT_SUFFIX = '-extract'

# high level archive "kinds"
docs = 1
regular = 2
regular_nested = 3
package = 4
file_system = 5
patches = 6
special_package = 7

kind_labels = {
    1: 'docs',
    2: 'regular',
    3: 'regular_nested',
    4: 'package',
    5: 'file_system',
    6: 'patches',
    7: 'special_package',
}

# note: we do not include special_package in all_kinds by default
all_kinds = (
    regular,
    regular_nested,
    package,
    file_system,
    docs,
    patches,
    special_package,
)

default_kinds = (
    regular,
    regular_nested,
    package,
)

# map user-visible extract types to tuples of "kinds"
extract_types = {
    'default': default_kinds,
    'all': all_kinds,
    'package': (package,),
    'filesystem': (file_system,),
    'doc': (docs,),
    'patch': (patches,),
    'special_package': (special_package,),
}


def is_extraction_path(path):
    """
    Return True is the path points to an extraction path.
    """
    return path and path.rstrip('\\/').endswith(EXTRACT_SUFFIX)


def is_extracted(location):
    """
    Return True is the location is already extracted to the corresponding
    extraction location.
    """
    return location and exists(get_extraction_path(location))


def get_extraction_path(path):
    """
    Return a path where to extract.
    """
    return path.rstrip('\\/') + EXTRACT_SUFFIX


def remove_archive_suffix(path):
    """
    Remove all the extracted suffix from a path.
    """
    return re.sub(EXTRACT_SUFFIX, '', path)


def remove_backslashes_and_dotdots(directory):
    """
    Walk a directory and rename the files if their names contain backslashes.
    Return a list of errors if any.
    """
    errors = []
    for top, _, files in os.walk(directory):
        for filename in files:
            if not ('\\' in filename or '..' in filename):
                continue
            try:
                new_path = as_posixpath(filename).strip('/')
                new_path = posixpath.normpath(new_path).replace('..', '/').strip('/')
                new_path = posixpath.normpath(new_path)
                segments = new_path.split('/')
                directory = join(top, *segments[:-1])
                create_dir(directory)
                shutil.move(join(top, filename), join(top, *segments))
            except Exception:
                errors.append(join(top, filename))
    return errors


def new_name(location, is_dir=False):
    """
    Return a new non-existing location from a `location` usable to write a file
    or create directory without overwriting existing files or directories in the
    same parent directory, ignoring the case of the filename.

    The case of the filename is ignored to ensure that similar results are
    returned across case sensitive (*nix) and case insensitive file systems.

    To find a new unique filename, this tries new names this way:
     * pad a directory name with _X where X is an incremented number.
     * pad a file base name with _X where X is an incremented number and keep
       the extension unchanged.
    """
    assert location
    location = location.rstrip('\\/')
    assert location

    parent = parent_directory(location)

    # all existing files or directory as lower case
    siblings_lower = set(s.lower() for s in os.listdir(parent))

    filename = file_name(location)

    # corner case
    if filename in ('.', '..'):
        filename = '_'

    # if unique, return this
    if filename.lower() not in siblings_lower:
        return join(parent, filename)

    # otherwise seek a unique name
    if is_dir:
        # directories do not have an "extension"
        base_name = filename
        ext = ''
    else:
        base_name, dot, ext = filename.partition('.')
        if dot:
            ext = f'.{ext}'
        else:
            base_name = filename
            ext = ''

    # find a unique filename, adding a counter int to the base_name
    counter = 1
    while 1:
        filename = f'{base_name}_{counter}{ext}'
        if filename.lower() not in siblings_lower:
            break
        counter += 1
    return join(parent, filename)


class ExtractError(Exception):
    pass


class ExtractErrorPasswordProtected(ExtractError):
    pass


class ExtractErrorFailedToExtract(ExtractError):
    pass


class ExtractWarningIncorrectEntry(ExtractError):
    pass


class ExtractWarningTrailingGarbage(ExtractError):
    pass
