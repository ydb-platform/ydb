# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

"""
Gets the current Facebook Python SDK version.
"""

import os
import re


def get_version():
    this_dir = os.path.dirname(__file__)
    package_init_filename = os.path.join(this_dir, '../__init__.py')

    version = None
    with open(package_init_filename, 'r') as handle:
        file_content = handle.read()
        version = re.search(
            r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
            file_content, re.MULTILINE
        ).group(1)

    if not version:
        raise ValueError('Cannot find version information')

    return version
