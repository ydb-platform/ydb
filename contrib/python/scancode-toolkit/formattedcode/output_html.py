#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import io
from operator import itemgetter
from os.path import abspath
from os.path import dirname
from os.path import exists
from os.path import expanduser
from os.path import isfile
from os.path import join

import click
import json

from commoncode.fileutils import as_posixpath
from commoncode.fileutils import copytree
from commoncode.fileutils import delete
from commoncode.fileutils import file_name
from commoncode.fileutils import file_base_name
from commoncode.fileutils import parent_directory
from formattedcode import FileOptionType
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OUTPUT_GROUP
from plugincode.output import output_impl
from plugincode.output import OutputPlugin

"""
Output plugins to write scan results using templates such as HTML.

Also contains a builtin to write scan results using a custom template
which is NOT a plugin
"""

TEMPLATES_DIR = join(dirname(__file__), 'templates')


@output_impl
class HtmlOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--html',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='Write scan output as HTML to FILE.',
            help_group=OUTPUT_GROUP,
            sort_order=50),
    ]

    def is_enabled(self, html, **kwargs):
        return html

    def process_codebase(self, codebase, html, **kwargs):
        results = self.get_files(codebase, **kwargs)
        version = codebase.get_or_create_current_header().tool_version
        template_loc = join(TEMPLATES_DIR, 'html', 'template.html')
        output_file = html
        write_templated(output_file, results, version, template_loc)


@output_impl
class CustomTemplateOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--custom-output',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            required_options=['custom_template'],
            metavar='FILE',
            help='Write scan output to FILE formatted with '
                 'the custom Jinja template file.',
            help_group=OUTPUT_GROUP,
            sort_order=60),

        PluggableCommandLineOption(('--custom-template',),
            type=click.Path(
                exists=True,
                file_okay=True,
                dir_okay=False,
                readable=True,
                path_type=str
            ),
            required_options=['custom_output'],
            metavar='FILE',
            help='Use this Jinja template FILE as a custom template.',
            help_group=OUTPUT_GROUP,
            sort_order=65),
    ]

    def is_enabled(self, custom_output, custom_template, **kwargs):
        return custom_output and custom_template

    def process_codebase(self, codebase, custom_output, custom_template, **kwargs):
        results = self.get_files(codebase, **kwargs)
        version = codebase.get_or_create_current_header().tool_version
        template_loc = custom_template
        output_file = custom_output
        write_templated(output_file, results, version, template_loc)


def write_templated(output_file, results, version, template_loc):
    """
    Write scan output `results` to the `output_file` opened file using a template
    file at `template_loc`.
    Raise an exception on errors.
    """
    template = get_template(template_loc)

    for template_chunk in generate_output(results, version, template):
        assert isinstance(template_chunk, str)
        try:
            output_file.write(template_chunk)
        except Exception:
            import traceback
            msg = 'ERROR: Failed to write output for: ' + repr(template_chunk)
            msg += '\n' + traceback.format_exc()
            raise Exception(msg)


def get_template(location):
    """
    Return a Jinja template object loaded from the file at `location`.
    """
    from jinja2 import Environment, FileSystemLoader

    location = as_posixpath(abspath(expanduser(location)))
    assert isfile(location)

    template_dir = parent_directory(location)
    env = Environment(loader=FileSystemLoader(template_dir))

    template_name = file_name(location)
    return env.get_template(template_name)


def generate_output(results, version, template):
    """
    Yield unicode strings from incrementally rendering `results` and `version`
    with the Jinja `template` object.
    """
    # FIXME: This code is highly coupled with actual scans and may not
    # support adding new scans at all

    from licensedcode.cache import get_licenses_db

    converted = {}
    converted_infos = {}
    converted_packages = {}
    licenses = {}

    LICENSES = 'licenses'
    COPYRIGHTS = 'copyrights'
    PACKAGES = 'packages'
    URLS = 'urls'
    EMAILS = 'emails'

    # Create a flattened data dict keyed by path
    for scanned_file in results:
        scanned_file = dict(scanned_file)
        path = scanned_file['path']
        results = []
        if COPYRIGHTS in scanned_file:
            for entry in scanned_file[COPYRIGHTS]:
                results.append({
                    'start': entry['start_line'],
                    'end': entry['end_line'],
                    'what': 'copyright',
                    'value': entry['value'],
                })
        if LICENSES in scanned_file:
            for entry in scanned_file[LICENSES]:
                # make copy
                entry = dict(entry)
                entry_key = entry['key']
                results.append({
                    'start': entry['start_line'],
                    'end': entry['end_line'],
                    'what': 'license',
                    'value': entry_key,
                })

                # FIXME: we should NOT rely on license objects: only use what is in the JSON instead
                if entry_key not in licenses:
                    licenses[entry_key] = entry
                    # we were modifying the scan data in place ....
                    entry['object'] = get_licenses_db().get(entry_key)
        if results:
            converted[path] = sorted(results, key=itemgetter('start'))

        # TODO: this is klunky: we need to drop templates entirely or we
        # should rather just pass a the list of files from the scan
        # results and let the template handle this rather than
        # denormalizing the list here??
        converted_infos[path] = {}
        for name, value in scanned_file.items():
            if name in (LICENSES, PACKAGES, COPYRIGHTS, EMAILS, URLS):
                continue
            converted_infos[path][name] = value

        if PACKAGES in scanned_file:
            converted_packages[path] = scanned_file[PACKAGES]

        licenses = dict(sorted(licenses.items()))

    files = {
        'license_copyright': converted,
        'infos': converted_infos,
        'packages': converted_packages
    }

    return template.generate(files=files, licenses=licenses, version=version)


@output_impl
class HtmlAppOutput(OutputPlugin):
    """
    Write scan output as a mini HTML application.
    """
    options = [
        PluggableCommandLineOption(('--html-app',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='(DEPRECATED: use the ScanCode Workbench app instead ) '
                  'Write scan output as a mini HTML application to FILE.',
            help_group=OUTPUT_GROUP,
            sort_order=1000),
    ]

    def is_enabled(self, html_app, **kwargs):
        return html_app

    def process_codebase(self, codebase, input, html_app, **kwargs):  # NOQA
        results = self.get_files(codebase, **kwargs)
        version = codebase.get_or_create_current_header().tool_version
        output_file = html_app
        scanned_path = input
        create_html_app(output_file, results, version, scanned_path)


class HtmlAppAssetCopyWarning(Exception):
    pass


class HtmlAppAssetCopyError(Exception):
    pass


def is_stdout(output_file):
    return output_file.name == '<stdout>'


def create_html_app(output_file, results, version, scanned_path):  # NOQA
    """
    Given an html-app output_file, generate that file, create the data.js data
    file from the results and create the corresponding `_files` directory and
    copy the data and assets to this directory. The target directory is deleted
    if it exists.

    Raise HtmlAppAssetCopyWarning if the output_file is <stdout> or
    HtmlAppAssetCopyError if the copy was not possible.
    """
    try:
        if is_stdout(output_file):
            raise HtmlAppAssetCopyWarning()

        source_assets_dir = join(TEMPLATES_DIR, 'html-app', 'assets')

        # Return a tuple of (parent_dir, dir_name) directory named after the
        # `output_location` output_locationfile_base_name (stripped from extension) and
        # a `_files` suffix Return empty strings if output is to stdout.
        output_location = output_file.name
        tgt_root_path = dirname(output_location)
        tgt_assets_dir = file_base_name(output_location) + '_files'

        # delete old assets
        target_assets_dir = join(tgt_root_path, tgt_assets_dir)
        if exists(target_assets_dir):
            delete(target_assets_dir)

        # copy assets
        copytree(source_assets_dir, target_assets_dir)

        template = get_template(join(TEMPLATES_DIR, 'html-app', 'template.html'))
        rendered_html = template.render(
            assets_dir=target_assets_dir,
            scanned_path=scanned_path,
            version=version
        )
        output_file.write(rendered_html)

        # create help file
        help_template = get_template(join(TEMPLATES_DIR, 'html-app', 'help_template.html'))
        rendered_help = help_template.render(main_app=output_location)
        with io.open(join(target_assets_dir, 'help.html'), 'w', encoding='utf-8') as f:
            f.write(rendered_help)

        # FIXME: this should a regular JSON scan format
        with io.open(join(target_assets_dir, 'data.js'), 'w') as f:
            f.write('data=')
            json.dump(list(results), f)

    except HtmlAppAssetCopyWarning as w:
        raise w

    except Exception as e:  # NOQA
        import traceback
        msg = 'ERROR: cannot create HTML application.\n' + traceback.format_exc()
        raise HtmlAppAssetCopyError(msg)
