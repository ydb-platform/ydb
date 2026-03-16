#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
from os.path import abspath
from os.path import basename
from os.path import dirname
from os.path import isdir
import sys

from io import BytesIO
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from spdx.checksum import Algorithm
from spdx.creationinfo import Tool
from spdx.document import Document
from spdx.document import License
from spdx.document import ExtractedLicense
from spdx.file import File
from spdx.package import Package
from spdx.utils import NoAssert
from spdx.utils import SPDXNone
from spdx.version import Version

from formattedcode import FileOptionType
from commoncode.cliutils import OUTPUT_GROUP
from commoncode.cliutils import PluggableCommandLineOption
from plugincode.output import output_impl
from plugincode.output import OutputPlugin

# Tracing flags
TRACE = False
TRACE_DEEP = False


def logger_debug(*args):
    pass


if TRACE or TRACE_DEEP:
    import logging

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str)
                                     and a or repr(a) for a in args))

"""
Output plugins to write scan results in SPDX format.
"""

_spdx_list_is_patched = False


def _patch_license_list():
    """
    Patch the SPDX library license list to match the list of ScanCode known SPDX
    licenses.
    """
    global _spdx_list_is_patched
    if not _spdx_list_is_patched:
        from spdx.config import LICENSE_MAP
        from licensedcode.models import load_licenses
        licenses = load_licenses(with_deprecated=True)
        spdx_licenses = get_licenses_by_spdx_key(licenses.values())
        LICENSE_MAP.update(spdx_licenses)
        _spdx_list_is_patched = True


def get_licenses_by_spdx_key(licenses):
    """
    Return a mapping of {spdx_key: license object} given a sequence of License
    objects.
    """
    spdx_licenses = {}
    for lic in licenses:
        if not (lic.spdx_license_key or lic.other_spdx_license_keys):
            continue

        if lic.spdx_license_key:
            name = lic.name
            slk = lic.spdx_license_key
            spdx_licenses[slk] = name
            spdx_licenses[name] = slk

            for other_spdx in lic.other_spdx_license_keys:
                if not (other_spdx and other_spdx.strip()):
                    continue
                slk = other_spdx
                spdx_licenses[slk] = name
                spdx_licenses[name] = slk

    return spdx_licenses


@output_impl
class SpdxTvOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--spdx-tv',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='Write scan output as SPDX Tag/Value to FILE.',
            help_group=OUTPUT_GROUP)
    ]

    def is_enabled(self, spdx_tv, **kwargs):
        return spdx_tv

    def process_codebase(self, codebase, spdx_tv, **kwargs):
        check_sha1(codebase)
        files = self.get_files(codebase, **kwargs)
        header = codebase.get_or_create_current_header()
        tool_name = header.tool_name
        tool_version = header.tool_version
        notice = header.notice
        input = kwargs.get('input', '')  # NOQA

        write_spdx(
            spdx_tv, files, tool_name, tool_version, notice, input, as_tagvalue=True)


@output_impl
class SpdxRdfOutput(OutputPlugin):

    options = [
        PluggableCommandLineOption(('--spdx-rdf',),
            type=FileOptionType(mode='w', encoding='utf-8', lazy=True),
            metavar='FILE',
            help='Write scan output as SPDX RDF to FILE.',
            help_group=OUTPUT_GROUP)
    ]

    def is_enabled(self, spdx_rdf, **kwargs):
        return spdx_rdf

    def process_codebase(self, codebase, spdx_rdf, **kwargs):
        check_sha1(codebase)
        files = self.get_files(codebase, **kwargs)
        header = codebase.get_or_create_current_header()
        tool_name = header.tool_name
        tool_version = header.tool_version
        notice = header.notice
        input = kwargs.get('input', '')  # NOQA

        write_spdx(
            spdx_rdf, files, tool_name, tool_version, notice, input, as_tagvalue=False)


def check_sha1(codebase):
    has_sha1 = hasattr(codebase.root, 'sha1')
    if not has_sha1:
        import click

        click.secho(
            'WARNING: Files are missing a SHA1 attribute. '
            'Incomplete SPDX document created.',
            err=True,
            fg='red')


def write_spdx(output_file, files, tool_name, tool_version, notice, input_file, as_tagvalue=True):
    """
    Write scan output as SPDX Tag/value or RDF.
    """
    as_rdf = not as_tagvalue
    _patch_license_list()
    absinput = abspath(input_file)

    if isdir(absinput):
        input_path = absinput
    else:
        input_path = dirname(absinput)

    doc = Document(Version(2, 1), License.from_identifier('CC0-1.0'))
    doc.comment = notice
    tool_name = tool_name or 'ScanCode'
    doc.creation_info.add_creator(Tool(tool_name + ' ' + tool_version))
    doc.creation_info.set_created_now()

    package = doc.package = Package(
        name=basename(input_path),
        download_location=NoAssert()
    )

    # Use a set of unique copyrights for the package.
    package.cr_text = set()

    all_files_have_no_license = True
    all_files_have_no_copyright = True

    # FIXME: this should walk the codebase instead!!!
    for file_data in files:

        # Skip directories.
        if file_data.get('type') != 'file':
            continue

        # Set a relative file name as that is what we want in
        # SPDX output (with explicit leading './').
        name = './' + file_data.get('path')
        file_entry = File(
            name=name,
            chk_sum=Algorithm('SHA1', file_data.get('sha1') or '')
        )

        file_licenses = file_data.get('licenses')
        if file_licenses:
            all_files_have_no_license = False
            for file_license in file_licenses:
                license_key = file_license.get('key')

                spdx_id = file_license.get('spdx_license_key')
                if not spdx_id:
                    spdx_id = 'LicenseRef-scancode-' + license_key
                is_license_ref = spdx_id.lower().startswith('licenseref-')

                if not is_license_ref:
                    spdx_license = License.from_identifier(spdx_id)
                else:
                    spdx_license = ExtractedLicense(spdx_id)
                    spdx_license.name = file_license.get('short_name')
                    comment = ('See details at https://github.com/nexB/scancode-toolkit'
                               '/blob/develop/src/licensedcode/data/licenses/%s.yml\n' % license_key)
                    spdx_license.comment = comment
                    text = file_license.get('matched_text')
                    # always set some text, even if we did not extract the matched text
                    if not text:
                        text = comment
                    spdx_license.text = text
                    doc.add_extr_lic(spdx_license)

                # Add licenses in the order they appear in the file. Maintaining the order
                # might be useful for provenance purposes.
                file_entry.add_lics(spdx_license)
                package.add_lics_from_file(spdx_license)

        elif file_licenses is None:
            all_files_have_no_license = False
            file_entry.add_lics(NoAssert())

        else:
            file_entry.add_lics(SPDXNone())

        file_entry.conc_lics = NoAssert()

        file_copyrights = file_data.get('copyrights')
        if file_copyrights:
            all_files_have_no_copyright = False
            file_entry.copyright = []
            for file_copyright in file_copyrights:
                file_entry.copyright.append(file_copyright.get('value'))

            package.cr_text.update(file_entry.copyright)

            # Create a text of copyright statements in the order they appear in the file.
            # Maintaining the order might be useful for provenance purposes.
            file_entry.copyright = '\n'.join(file_entry.copyright) + '\n'

        elif file_copyrights is None:
            all_files_have_no_copyright = False
            file_entry.copyright = NoAssert()

        else:
            file_entry.copyright = SPDXNone()

        package.add_file(file_entry)

    if len(package.files) == 0:
        if as_tagvalue:
            msg = "# No results for package '{}'.\n".format(package.name)
        else:
            # rdf
            msg = "<!-- No results for package '{}'. -->\n".format(package.name)
        output_file.write(msg)

    # Remove duplicate licenses from the list for the package.
    unique_licenses = {(l.identifier, l.full_name): l for l in package.licenses_from_files}
    unique_licenses = list(unique_licenses.values())
    if not len(package.licenses_from_files):
        if all_files_have_no_license:
            package.licenses_from_files = [SPDXNone()]
        else:
            package.licenses_from_files = [NoAssert()]
    else:
        # List license identifiers alphabetically for the package.
        package.licenses_from_files = sorted(unique_licenses, key=lambda x: x.identifier)

    if len(package.cr_text) == 0:
        if all_files_have_no_copyright:
            package.cr_text = SPDXNone()
        else:
            package.cr_text = NoAssert()
    else:
        # Create a text of alphabetically sorted copyright
        # statements for the package.
        package.cr_text = '\n'.join(sorted(package.cr_text)) + '\n'

    package.verif_code = doc.package.calc_verif_code()
    package.license_declared = NoAssert()
    package.conc_lics = NoAssert()

    # The spdx-tools write_document returns either:
    # - unicode for tag values
    # - UTF8-encoded bytes for rdf because somehow the rdf and xml
    #   libraries do the encoding and do not return text but bytes
    # The file passed by ScanCode for output is opened in text mode Therefore in
    # one case we do need to deal with bytes and decode before writing (rdf) and
    # in the other case we deal with text all the way.

    if package.files:

        if as_tagvalue:
            from spdx.writers.tagvalue import write_document  # NOQA
        elif as_rdf:
            from spdx.writers.rdf import write_document  # NOQA

        if as_tagvalue:
            spdx_output = StringIO()
        elif as_rdf:
            # rdf is utf-encoded bytes
            spdx_output = BytesIO()

        write_document(doc, spdx_output, validate=False)
        result = spdx_output.getvalue()

        if as_rdf:
            # rdf is utf-encoded bytes
            result = result.decode('utf-8')

        output_file.write(result)
