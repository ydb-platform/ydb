#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from commoncode import fileutils

"""
Recognition of typical "legal" files such as "LICENSE", "COPYING", etc.
"""

special_names = (
    'COPYING',
    'COPYRIGHT',
    'COPYRIGHTS',
    'NOTICE',
    'NOTICES',
    'LICENSE',
    'LICENCE',
    'LICENSES',
    'LICENCES',
    'LICENSING',
    'LICENCING',
    'COPYLEFT',
    'LEGAL',
    'EULA',
    'AGREEMENT',
    'AGREEMENTS',
    'ABOUT',
    'UNLICENSE',
    'COMMITMENT',
    'COMMITMENTS',
    'WARRANTY',
    'COPYLEFT',
)


special_names_lower = tuple(x.lower() for x in special_names)


def is_special_legal_file(location):
    """
    Return an indication that a file may be a "special" legal-like file.
    """
    file_base_name = fileutils.file_base_name(location)
    file_base_name_lower = file_base_name.lower()
    file_extension = fileutils.file_extension(location)
    file_extension_lower = file_extension.lower()

    name_contains_special = (
        special_name in file_base_name or
        special_name in file_extension
            for special_name in special_names
    )

    name_lower_is_special = (
        special_name_lower in (file_base_name_lower, file_extension_lower)
            for special_name_lower in special_names_lower
    )

    name_lower_contains_special = (
        special_name_lower in file_base_name_lower or
        special_name_lower in file_extension_lower
            for special_name_lower in special_names_lower
    )

    if any(name_contains_special) or any(name_lower_is_special):
        return 'yes'

    elif any(name_lower_contains_special):
        return 'maybe'
    else:
        # return False for now?
        pass
