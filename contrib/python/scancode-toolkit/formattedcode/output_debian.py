#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.

from debian_inspector.copyright import CopyrightFilesParagraph
from debian_inspector.copyright import CopyrightHeaderParagraph
from debian_inspector.copyright import DebianCopyright
from license_expression import Licensing

from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OUTPUT_GROUP
from formattedcode import FileOptionType
from plugincode.output import output_impl
from plugincode.output import OutputPlugin

from scancode import notice

"""
Output plugin to write scan results as Debian machine-readable copyright format
a.k.a. DEP5

See https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/ for details
"""


@output_impl
class DebianCopyrightOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--debian', 'output_debian',),
            type=FileOptionType(mode='w', lazy=True),
            metavar='FILE',
            help='Write scan output in machine-readable Debian copyright format to FILE.',
            help_group=OUTPUT_GROUP,

            # this is temporary , we should not needed these options explicitly
            # but instead adapt to the available data
            required_options=['copyright', 'license', 'license_text'],
            sort_order=60),
    ]

    def is_enabled(self, output_debian, **kwargs):
        return output_debian

    def process_codebase(self, codebase, output_debian, **kwargs):
        debian_copyright = build_debian_copyright(codebase, **kwargs)
        write_debian_copyright(
            debian_copyright=debian_copyright,
            output_file=output_debian,
        )


def write_debian_copyright(debian_copyright, output_file):
    """
    Write `debian_copyright` DebianCopyright object to the `output_file` opened
    file-like object.
    """
    # note the output_closing may feel contrived but is more or less dictated
    # by the Click behaviour
    close_of = False
    try:
        if isinstance(output_file, str):
            output_file = open(output_file, 'w')
            close_of = True
        output_file.write(debian_copyright.dumps())
        output_file.write('\n')
    finally:
        if close_of:
            output_file.close()


def build_debian_copyright(codebase, **kwargs):
    """
    Return a DebianCopyrightobject built from the codebase.
    """
    paragraphs = list(build_copyright_paragraphs(codebase, **kwargs))
    return DebianCopyright(paragraphs=paragraphs)


def build_copyright_paragraphs(codebase, **kwargs):
    """
    Yield paragraphs built from the codebase.
    The codebase is assumed to contains license and copyright detections.
    """

    codebase.add_files_count_to_current_header()
    header_para = CopyrightHeaderParagraph(
        format='https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/',
        # TODO: add some details, but not all these
        # comment=saneyaml.dump(codebase.get_headers()),
        comment=notice,
    )
    yield header_para

    # TODO: create CopyrightLicenseParagraph for common licenses
    # TODO: group files that share copyright and license
    # TODO: infer files patternsas in decopy

    # for now this is dumb and will generate one paragraph per scanned file
    for scanned_file in OutputPlugin.get_files(codebase, **kwargs):
        if scanned_file['type'] == 'directory':
            continue
        dfiles = scanned_file['path']
        dlicense = build_license(scanned_file)
        dcopyright = build_copyright_field(scanned_file)

        file_para = CopyrightFilesParagraph.from_dict(dict(
            files=dfiles,
            license=dlicense,
            copyright=dcopyright,
        ))

        yield file_para


def build_copyright_field(scanned_file):
    """
    Return a CopyrightField from the copyright statements detected in
    `scanned_file` or None if no copyright is detected.
    """
    # TODO: format copyright notices the same way Debian does
    holders = scanned_file.get('holders', []) or []
    if not holders:
        return
    # TODO: reinjects copyright year ranges like they show up in Debian
    statements = [h['value'] for h in holders]
    return '\n'.join(statements)


def build_license(scanned_file):
    """
    Return Debian-like text where the first line is the expression and the
    remaining lines are the license text from licenses detected in
    `scanned_file` or None if no license is detected.
    """
    # TODO: filter based on license scores and/or add warnings and or detailed comments with that info
    license_expressions = scanned_file.get('license_expressions', [])
    if not license_expressions:
        return

    # TODO: use either Debian license symbols or SPDX
    # TODO: convert license expression to Debian style of expressions
    expression = combine_expressions(license_expressions)

    licenses = scanned_file.get('licenses', [])
    text = '\n'.join(get_texts(licenses))
    return f'{expression}\n{text}'


# TODO: this has no readon to be here and should be part of the license_expression library
def combine_expressions(expressions, relation='AND', licensing=Licensing()):
    """
    Return a combined license expression string with relation, given a list of
    license expressions strings.

    For example:
    >>> a = 'mit'
    >>> b = 'gpl'
    >>> combine_expressions([a, b])
    'mit AND gpl'
    >>> assert 'mit' == combine_expressions([a])
    >>> combine_expressions([])
    >>> combine_expressions(None)
    >>> combine_expressions(('gpl', 'mit', 'apache',))
    'gpl AND mit AND apache'
    """
    if not expressions:
        return

    if not isinstance(expressions, (list, tuple)):
        raise TypeError(
            'expressions should be a list or tuple and not: {}'.format(
                type(expressions)))

    # Remove duplicate element in the expressions list
    expressions = list(dict((x, True) for x in expressions).keys())

    if len(expressions) == 1:
        return expressions[0]

    expressions = [licensing.parse(le, simple=True) for le in expressions]
    if relation == 'OR':
        return str(licensing.OR(*expressions))
    else:
        return str(licensing.AND(*expressions))


def get_texts(detected_licenses):
    """
    Yield license texts detected in this file.

    The current data structure has this format, with one item for each license
    key from each license expression detected:

    licenses": [
        {
          "key": "mit-old-style",
          "score": 100.0,
           ...
          "start_line": 9,
          "end_line": 15,
          "matched_rule": {
            "identifier": "mit-old-style_cmr-no_1.RULE",
            "license_expression": "mit-old-style",
            ...
          },
          "matched_text": "Permission to use, copy, modify, ...."
        }
      ],
    """
    # FIXME: the current license data structure is contrived and will soon be
    # streamlined. See https://github.com/nexB/scancode-toolkit/issues/2416

    # set of (start line, end line, matched_rule identifier)
    seen = set()
    for lic in detected_licenses:
        key = lic['start_line'], lic['end_line'], lic['matched_rule']['identifier']
        if key not in seen:
            yield lic['matched_text']
            seen.add(key)

