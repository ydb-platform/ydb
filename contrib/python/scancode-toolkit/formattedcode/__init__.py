#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import os
from itertools import chain

import click


class FileOptionType(click.File):
    """
    A click.File subclass that ensures that a file name is not set to an
    existing option parameter to avoid mistakes.
    """

    def convert(self, value, param, ctx):
        known_opts = set(chain.from_iterable(
            p.opts for p in ctx.command.params if isinstance(p, click.Option)))
        if value in known_opts:
            self.fail(
                'Illegal file name conflicting with an option name: '
                f'{ os.fsdecode(value)}. '
                'Use the special "-" file name to print results on screen/stdout.',
                param,
                ctx,
            )
        return click.File.convert(self, value, param, ctx)
