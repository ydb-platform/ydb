#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import json

from formattedcode import FileOptionType
from commoncode.cliutils import OUTPUT_GROUP
from commoncode.cliutils import PluggableCommandLineOption
from plugincode.output import OutputPlugin
from plugincode.output import output_impl

"""
Output plugin to write scan results as JSON lines.
"""


@output_impl
class JsonLinesOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--json-lines', 'output_json_lines',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='Write scan output as JSON Lines to FILE.',
            help_group=OUTPUT_GROUP,
            sort_order=15),
    ]

    def is_enabled(self, output_json_lines, **kwargs):
        return output_json_lines

    # TODO: reuse the json output code and merge that in a single plugin
    def process_codebase(self, codebase, output_json_lines, **kwargs):
        # NOTE: we write as binary, not text
        files = self.get_files(codebase, **kwargs)

        codebase.add_files_count_to_current_header()

        headers = dict(headers=codebase.get_headers())

        compact_separators = (u',', u':',)
        output_json_lines.write(
            json.dumps(headers, separators=compact_separators))
        output_json_lines.write('\n')

        for name, value in codebase.attributes.to_dict().items():
            if value:
                smry = {name: value}
                output_json_lines.write(
                    json.dumps(smry, separators=compact_separators))
                output_json_lines.write('\n')

        for scanned_file in files:
            scanned_file_line = {'files': [scanned_file]}
            output_json_lines.write(
                json.dumps(scanned_file_line, separators=compact_separators))
            output_json_lines.write('\n')
