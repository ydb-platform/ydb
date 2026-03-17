#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import jsonstreams

from formattedcode import FileOptionType
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OUTPUT_GROUP
from plugincode.output import output_impl
from plugincode.output import OutputPlugin

"""
Output plugins to write scan results as JSON.
"""

# Tracing flags
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import sys
    import logging

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str)
                                     and a or repr(a) for a in args))


@output_impl
class JsonCompactOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--json', 'output_json',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='Write scan output as compact JSON to FILE.',
            help_group=OUTPUT_GROUP,
            sort_order=10),
    ]

    def is_enabled(self, output_json, **kwargs):
        return output_json

    def process_codebase(self, codebase, output_json, **kwargs):
        write_results(codebase, output_file=output_json, pretty=False, **kwargs)


@output_impl
class JsonPrettyOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--json-pp', 'output_json_pp',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='Write scan output as pretty-printed JSON to FILE.',
            help_group=OUTPUT_GROUP,
            sort_order=10),
    ]

    def is_enabled(self, output_json_pp, **kwargs):
        return output_json_pp

    def process_codebase(self, codebase, output_json_pp, **kwargs):
        write_results(codebase, output_file=output_json_pp, pretty=True, **kwargs)


def write_results(codebase, output_file, pretty=False, **kwargs):
    """
    Write headers, files, and other attributes from `codebase` to `output_file`

    Enable JSON indentation if `pretty` is True
    """
    # Set indentation for JSON output if `pretty` is True
    # We use a separate dict for jsonstream kwargs since we are passing
    # this function's kwargs as arguments to OutputPlugin.get_files()
    if pretty:
        jsonstreams_kwargs = dict(indent=2, pretty=True)
    else:
        jsonstreams_kwargs = dict(indent=None, pretty=False)

    # If `output_file` is a path string, open the file at path `output_file` and use it as `output_file`
    close_fd = False
    if isinstance(output_file, str):
        output_file = open(output_file, 'w')
        close_fd = True

    # Begin wri'w' JSON to `output_file`
    with jsonstreams.Stream(jsonstreams.Type.object, fd=output_file, close_fd=close_fd, **jsonstreams_kwargs) as s:
        # Write headers
        codebase.add_files_count_to_current_header()
        codebase_headers = codebase.get_headers()
        s.write('headers', codebase_headers)

        # Write attributes
        if codebase.attributes:
            for attribute_key, attribute_value in codebase.attributes.to_dict().items():
                s.write(attribute_key, attribute_value)

        # Write files
        codebase_files = OutputPlugin.get_files(codebase, **kwargs)
        # OutputPlugin.get_files() returns a generator, not JSON-serializable
        codebase_files = list(codebase_files)
        s.write('files', codebase_files)


def get_results(codebase, as_list=False, **kwargs):
    """
    Return an ordered mapping of scan results collected from a `codebase`.
    if `as_list` consume the "files" iterator in a list sequence.
    """

    codebase.add_files_count_to_current_header()
    results = dict([('headers', codebase.get_headers()), ])

    # add codebase toplevel attributes such as summaries
    if codebase.attributes:
        results.update(codebase.attributes.to_dict())

    files = OutputPlugin.get_files(codebase, **kwargs)
    if as_list:
        files = list(files)
    results['files'] = files

    if TRACE:
        logger_debug('get_results: files')
        files = list(files)
        from pprint import pformat
        logger_debug(pformat(files))

    return results
