#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
from collections import namedtuple
from itertools import chain
from os import path

import click
from click.types import BoolParamType

from commoncode import fileutils

# Tracing flags
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging
    import sys
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str)
                                     and a or repr(a) for a in args))


class ScancodeError(Exception):
    """Base exception for scancode errors"""


class ScancodeCliUsageError(ScancodeError, click.UsageError):
    """Exception for command line usage errors"""


# Holds a scan plugin result "key and the corresponding function.
# click.Parameter instance
Scanner = namedtuple('Scanner', 'name function')

notice = '''Generated with ScanCode and provided on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied. No content created from
ScanCode should be considered or used as legal advice. Consult an Attorney
for any legal advice.
ScanCode is a free software code scanning tool from nexB Inc. and others.
Visit https://github.com/nexB/scancode-toolkit/ for support and download.'''


def print_about(ctx, param, value):
    """
    Click callback to print a full notice.
    """
    if not value or ctx.resilient_parsing:
        return
    info_text = '''
ScanCode scans code and other files for origin and license.
Visit https://www.aboutcode.org/ and https://github.com/nexB/scancode-toolkit/
for support and download.

'''

    with open(path.join(path.abspath(path.dirname(__file__)), 'NOTICE')) as n:
        notice_text = n.read()
    click.echo(info_text + notice_text)
    ctx.exit()
