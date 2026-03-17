
# Copyright (c) 2014 Ahmed H. Ismail
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import six
from six.moves import zip_longest

from spdx import document
from spdx import file as spdx_file


class InvalidDocumentError(Exception):
    """
    Raised when attempting to write an invalid document.
    """
    pass


def write_separators(out):
    out.write(u'\n' * 2)


def format_verif_code(package):
    if len(package.verif_exc_files) == 0:
        return package.verif_code
    else:
        return "{0} ({1})".format(package.verif_code, ','.join(package.verif_exc_files))


def write_value(tag, value, out):
    out.write(u'{0}: {1}\n'.format(tag, value))


def write_text_value(tag, value, out):
    value = u'{0}: <text>{1}</text>\n'.format(tag, value)
    out.write(value)


def write_creation_info(creation_info, out):
    """
    Write the creation info to out.
    """
    out.write('# Creation Info\n\n')
    # Write sorted creators
    for creator in sorted(creation_info.creators):
        write_value('Creator', creator, out)

    # write created
    write_value('Created', creation_info.created_iso_format, out)
    # possible comment
    if creation_info.has_comment:
        write_text_value('CreatorComment', creation_info.comment, out)


def write_review(review, out):
    """
    Write the fields of a single review to out.
    """
    out.write('# Review\n\n')
    write_value('Reviewer', review.reviewer, out)
    write_value('ReviewDate', review.review_date_iso_format, out)
    if review.has_comment:
        write_text_value('ReviewComment', review.comment, out)


def write_annotation(annotation, out):
    """
    Write the fields of a single annotation to out.
    """
    out.write('# Annotation\n\n')
    write_value('Annotator', annotation.annotator, out)
    write_value('AnnotationDate', annotation.annotation_date_iso_format, out)
    if annotation.has_comment:
        write_text_value('AnnotationComment', annotation.comment, out)
    write_value('AnnotationType', annotation.annotation_type, out)
    if annotation.spdx_id:
        write_value('SPDXREF', annotation.spdx_id, out)


def write_file_type(ftype, out):
    VALUES = {
        spdx_file.FileType.SOURCE: 'SOURCE',
        spdx_file.FileType.OTHER: 'OTHER',
        spdx_file.FileType.BINARY: 'BINARY',
        spdx_file.FileType.ARCHIVE: 'ARCHIVE'
    }
    write_value('FileType', VALUES[ftype], out)


def write_file(spdx_file, out):
    """
    Write a file fields to out.
    """
    out.write('# File\n\n')
    write_value('FileName', spdx_file.name, out)
    if spdx_file.spdx_id:
        write_value('SPDXID', spdx_file.spdx_id, out)
    if spdx_file.has_optional_field('type'):
        write_file_type(spdx_file.type, out)
    write_value('FileChecksum', spdx_file.chk_sum.to_tv(), out)
    if isinstance(spdx_file.conc_lics, (document.LicenseConjunction, document.LicenseDisjunction)):
        write_value('LicenseConcluded', u'({0})'.format(spdx_file.conc_lics), out)
    else:
        write_value('LicenseConcluded', spdx_file.conc_lics, out)

    # write sorted list
    for lics in sorted(spdx_file.licenses_in_file):
        write_value('LicenseInfoInFile', lics, out)

    if isinstance(spdx_file.copyright, six.string_types):
        write_text_value('FileCopyrightText', spdx_file.copyright, out)
    else:
        write_value('FileCopyrightText', spdx_file.copyright, out)

    if spdx_file.has_optional_field('license_comment'):
        write_text_value('LicenseComments', spdx_file.license_comment, out)

    if spdx_file.has_optional_field('comment'):
        write_text_value('FileComment', spdx_file.comment, out)

    if spdx_file.has_optional_field('notice'):
        write_text_value('FileNotice', spdx_file.notice, out)

    for contributor in sorted(spdx_file.contributors):
        write_value('FileContributor', contributor, out)

    for dependency in sorted(spdx_file.dependencies):
        write_value('FileDependency', dependency, out)

    names = spdx_file.artifact_of_project_name
    homepages = spdx_file.artifact_of_project_home
    uris = spdx_file.artifact_of_project_uri

    for name, homepage, uri in sorted(zip_longest(names, homepages, uris)):
        write_value('ArtifactOfProjectName', name, out)
        if homepage is not None:
            write_value('ArtifactOfProjectHomePage', homepage, out)
        if uri is not None:
            write_value('ArtifactOfProjectURI', uri, out)


def write_snippet(snippet, out):
    """
    Write snippet fields to out.
    """
    out.write('# Snippet\n\n')
    write_value('SnippetSPDXID', snippet.spdx_id, out)
    write_value('SnippetFromFileSPDXID', snippet.snip_from_file_spdxid, out)
    write_text_value('SnippetCopyrightText', snippet.copyright, out)
    if snippet.has_optional_field('name'):
        write_value('SnippetName', snippet.name, out)
    if snippet.has_optional_field('comment'):
        write_text_value('SnippetComment', snippet.comment, out)
    if snippet.has_optional_field('license_comment'):
        write_text_value('SnippetLicenseComments', snippet.license_comment, out)
    if isinstance(snippet.conc_lics,
                  (document.LicenseConjunction, document.LicenseDisjunction)):
        write_value('SnippetLicenseConcluded', u'({0})'.format(
            snippet.conc_lics), out)
    else:
        write_value('SnippetLicenseConcluded', snippet.conc_lics, out)
    # Write sorted list
    for lics in sorted(snippet.licenses_in_snippet):
        write_value('LicenseInfoInSnippet', lics, out)


def write_package(package, out):
    """
    Write a package fields to out.
    """
    out.write('# Package\n\n')
    if package.name:
        write_value('PackageName', package.name, out)
    if package.spdx_id:
        write_value('SPDXID', package.spdx_id, out)
    if package.has_optional_field('version'):
        write_value('PackageVersion', package.version, out)
    write_value('PackageDownloadLocation', package.download_location, out)

    if package.has_optional_field('files_analyzed'):
        write_value('FilesAnalyzed', package.files_analyzed, out)

    if package.has_optional_field('summary'):
        write_text_value('PackageSummary', package.summary, out)

    if package.has_optional_field('source_info'):
        write_text_value('PackageSourceInfo', package.source_info, out)

    if package.has_optional_field('file_name'):
        write_value('PackageFileName', package.file_name, out)

    if package.has_optional_field('supplier'):
        write_value('PackageSupplier', package.supplier, out)

    if package.has_optional_field('originator'):
        write_value('PackageOriginator', package.originator, out)

    if package.has_optional_field('check_sum'):
        write_value('PackageChecksum', package.check_sum.to_tv(), out)

    write_value('PackageVerificationCode', format_verif_code(package), out)

    if package.has_optional_field('description'):
        write_text_value('PackageDescription', package.description, out)

    if package.has_optional_field('comment'):
        write_text_value('PackageComment', package.comment, out)

    if isinstance(package.license_declared, (document.LicenseConjunction,
        document.LicenseDisjunction)):
        write_value('PackageLicenseDeclared', u'({0})'.format(package.license_declared), out)
    else:
        write_value('PackageLicenseDeclared', package.license_declared, out)

    if isinstance(package.conc_lics, (document.LicenseConjunction,
        document.LicenseDisjunction)):
        write_value('PackageLicenseConcluded', u'({0})'.format(package.conc_lics), out)
    else:
        write_value('PackageLicenseConcluded', package.conc_lics, out)

    # Write sorted list of licenses.
    for lics in sorted(package.licenses_from_files):
        write_value('PackageLicenseInfoFromFiles', lics, out)

    if package.has_optional_field('license_comment'):
        write_text_value('PackageLicenseComments', package.license_comment, out)

    # cr_text is either free form text or NONE or NOASSERTION.
    if package.cr_text:
        if isinstance(package.cr_text, six.string_types):
            write_text_value('PackageCopyrightText', package.cr_text, out)
        else:
            write_value('PackageCopyrightText', package.cr_text, out)

    if package.has_optional_field('homepage'):
        write_value('PackageHomePage', package.homepage, out)

    for pkg_ref in package.pkg_ext_refs:
        pkg_ref_str = ' '.join([pkg_ref.category,
                                pkg_ref.pkg_ext_ref_type,
                                pkg_ref.locator])
        write_value('ExternalRef', pkg_ref_str, out)
        if pkg_ref.comment:
            write_text_value('ExternalRefComment', pkg_ref.comment, out)

    # Write sorted files.
    for spdx_file in sorted(package.files):
        write_separators(out)
        write_file(spdx_file, out)


def write_extracted_licenses(lics, out):
    """
    Write extracted licenses fields to out.
    """
    write_value('LicenseID', lics.identifier, out)

    if lics.full_name is not None:
        write_value('LicenseName', lics.full_name, out)

    if lics.comment is not None:
        write_text_value('LicenseComment', lics.comment, out)

    for xref in sorted(lics.cross_ref):
        write_value('LicenseCrossReference', xref, out)

    write_text_value('ExtractedText', lics.text, out)


def write_document(document, out, validate=True):
    """
    Write an SPDX tag value document.
    - document - spdx.document instance.
    - out - file like object that will be written to.
    Optionally `validate` the document before writing and raise
    InvalidDocumentError if document.validate returns False.
    """
    messages = []
    messages = document.validate(messages)
    if validate and messages:
        raise InvalidDocumentError(messages)

    # Write out document information
    out.write('# Document Information\n\n')
    write_value('SPDXVersion', str(document.version), out)
    write_value('DataLicense', document.data_license.identifier, out)
    if document.name:
        write_value('DocumentName', document.name, out)
    write_value('SPDXID', 'SPDXRef-DOCUMENT', out)
    if document.namespace:
        write_value('DocumentNamespace', document.namespace, out)
    if document.has_comment:
        write_text_value('DocumentComment', document.comment, out)
    for doc_ref in document.ext_document_references:
        doc_ref_str = ' '.join([doc_ref.external_document_id,
                                doc_ref.spdx_document_uri,
                                doc_ref.check_sum.identifier + ':' +
                                doc_ref.check_sum.value])
        write_value('ExternalDocumentRef', doc_ref_str, out)
    write_separators(out)
    # Write out creation info
    write_creation_info(document.creation_info, out)
    write_separators(out)

    # Writesorted reviews
    for review in sorted(document.reviews):
        write_review(review, out)
        write_separators(out)

    # Write sorted annotations
    for annotation in sorted(document.annotations):
        write_annotation(annotation, out)
        write_separators(out)

    # Write out package info
    write_package(document.package, out)
    write_separators(out)

    # Write out snippet info
    for snippet in document.snippet:
        write_snippet(snippet, out)
        write_separators(out)

    out.write('# Extracted Licenses\n\n')
    for lic in sorted(document.extracted_licenses):
        write_extracted_licenses(lic, out)
        write_separators(out)
