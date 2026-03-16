# Copyright 2019-2026 Michael Samoglyadov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Patch tool for nested diff."""

import argparse
import sys

import nested_diff.cli

HELP_EPILOG = """\
examples:
  patch document:
    %(prog)s target.json patch.json

  redefine serialization options:
    %(prog)s --ofmt json --ofmt-opts '{"indent": null}' target.json patch.json
"""


class App(nested_diff.cli.App):
    """Patch tool for nested structures."""

    def get_dumper(self, fmt, **kwargs):
        """Create dumper object according to passed format.

        Args:
            fmt: Dumper format.
            kwargs: Passed to dumper's constructor as is.

        Returns:
            Dumper object.

        """
        if fmt == 'auto':
            fmt = self.guess_fmt(self.args.target_file, 'json')

        return super().get_dumper(fmt, **kwargs)

    def get_positional_args_parser(self):
        """Return parser for positional part of CLI args."""
        parser = super().get_positional_args_parser()

        parser.add_argument(
            'target_file',
            type=argparse.FileType('r+'),
            help='file to patch',
        )
        parser.add_argument(
            'patch_file',
            nargs='?',
            default=sys.stdin,
            type=argparse.FileType(),
            help='optional, STDIN used when omitted',
        )

        return parser

    @staticmethod
    def patch(target, diff):
        """Patch object using nested diff..

        Args:
            target: Object to patch.
            diff: Nested diff.

        Returns:
            Patched object.

        """
        return nested_diff.Patcher().patch(target, diff)

    def run(self):
        """Patch app entry point."""
        patched = self.patch(
            self.load(self.args.target_file),
            self.load(self.args.patch_file),
        )

        self.args.target_file.seek(0)
        self.dumper.dump(self.args.target_file, patched)
        self.args.target_file.truncate()

        return 0
