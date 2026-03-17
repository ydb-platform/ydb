
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

from functools import total_ordering

from spdx import config


@total_ordering
class ExternalDocumentRef(object):
    """
    External Document References entity that contains the following fields :
    - external_document_id: A unique string containing letters, numbers, '.',
        '-' or '+'.
    - spdx_document_uri: The unique ID of the SPDX document being referenced.
    - check_sum: The checksum of the referenced SPDX document.
    """

    def __init__(self, external_document_id=None, spdx_document_uri=None,
                 check_sum=None):
        self.external_document_id = external_document_id
        self.spdx_document_uri = spdx_document_uri
        self.check_sum = check_sum

    def __eq__(self, other):
        return (
            isinstance(other, ExternalDocumentRef)
            and self.external_document_id == other.external_document_id
            and self.spdx_document_uri == other.spdx_document_uri
            and self.check_sum == other.check_sum
        )

    def __lt__(self, other):
        return (
            (self.external_document_id, self.spdx_document_uri,
             self.check_sum) <
            (other.external_document_id, other.spdx_document_uri,
             other.check_sum,)
        )

    def validate(self, messages):
        """
        Validate all fields of the ExternalDocumentRef class and update the
        messages list with user friendly error messages for display.
        """
        messages = self.validate_ext_doc_id(messages)
        messages = self.validate_spdx_doc_uri(messages)
        messages = self.validate_checksum(messages)

        return messages

    def validate_ext_doc_id(self, messages):
        if not self.external_document_id:
            messages = messages + [
                'ExternalDocumentRef has no External Document ID.'
            ]

        return messages

    def validate_spdx_doc_uri(self, messages):
        if not self.spdx_document_uri:
            messages = messages + [
                'ExternalDocumentRef has no SPDX Document URI.'
            ]

        return messages

    def validate_checksum(self, messages):
        if not self.check_sum:
            messages = messages + ['ExternalDocumentRef has no Checksum.']

        return messages


def _add_parens(required, text):
    """
    Add parens around a license expression if `required` is True, otherwise
    return `text` unmodified.
    """
    return '({})'.format(text) if required else text


@total_ordering
class License(object):
    def __init__(self, full_name, identifier):
        self._full_name = full_name
        self._identifier = identifier

    @classmethod
    def from_identifier(cls, identifier):
        """If identifier exists in config.LICENSE_MAP
        the full_name is retrieved from it. Otherwise
        the full_name is the same as the identifier.
        """
        if identifier in config.LICENSE_MAP.keys():
            return cls(config.LICENSE_MAP[identifier], identifier)
        else:
            return cls(identifier, identifier)

    @classmethod
    def from_full_name(cls, full_name):
        """
        Returna new License for a full_name. If the full_name exists in
        config.LICENSE_MAP the identifier is retrieved from it.
        Otherwise the identifier is the same as the full_name.
        """
        if full_name in config.LICENSE_MAP.keys():
            return cls(full_name, config.LICENSE_MAP[full_name])
        else:
            return cls(full_name, full_name)

    @property
    def url(self):
        return "http://spdx.org/licenses/{0}".format(self.identifier)

    @property
    def full_name(self):
        return self._full_name

    @full_name.setter
    def full_name(self, value):
        self._full_name = value

    @property
    def identifier(self):
        return self._identifier

    def __eq__(self, other):
        return (
            isinstance(other, License)
            and self.identifier == other.identifier
            and self.full_name == other.full_name)

    def __lt__(self, other):
        return isinstance(other, License) and self.identifier < other.identifier

    def __str__(self):
        return self.identifier

    def __hash__(self):
        return self.identifier.__hash__()


class LicenseConjunction(License):
    """
    A conjunction of two licenses.
    """

    def __init__(self, license_1, license_2):
        self.license_1 = license_1
        self.license_2 = license_2
        super(LicenseConjunction, self).__init__(self.full_name, self.identifier)

    @property
    def full_name(self):
        license_1_complex = type(self.license_1) == LicenseDisjunction
        license_2_complex = type(self.license_2) == LicenseDisjunction

        return '{0} AND {1}'.format(
            _add_parens(license_1_complex, self.license_1.full_name),
            _add_parens(license_2_complex, self.license_2.full_name))

    @property
    def identifier(self):
        license_1_complex = type(self.license_1) == LicenseDisjunction
        license_2_complex = type(self.license_2) == LicenseDisjunction

        return '{0} AND {1}'.format(
            _add_parens(license_1_complex, self.license_1.identifier),
            _add_parens(license_2_complex, self.license_2.identifier))


class LicenseDisjunction(License):
    """
    A disjunction of two licenses.
    """

    def __init__(self, license_1, license_2):
        self.license_1 = license_1
        self.license_2 = license_2
        super(LicenseDisjunction, self).__init__(self.full_name, self.identifier)

    @property
    def full_name(self):
        license_1_complex = type(self.license_1) == LicenseConjunction
        license_2_complex = type(self.license_2) == LicenseConjunction

        return '{0} OR {1}'.format(
            _add_parens(license_1_complex, self.license_1.full_name),
            _add_parens(license_2_complex, self.license_2.full_name))

    @property
    def identifier(self):
        license_1_complex = type(self.license_1) == LicenseConjunction
        license_2_complex = type(self.license_2) == LicenseConjunction

        return '{0} OR {1}'.format(
            _add_parens(license_1_complex, self.license_1.identifier),
            _add_parens(license_2_complex, self.license_2.identifier))


@total_ordering
class ExtractedLicense(License):
    """
    Represent an ExtractedLicense with its additional attributes:
    - text: Extracted text, str. Mandatory.
    - cross_ref: list of cross references.
    - comment: license comment, str.
    - full_name: license name. str or utils.NoAssert.
    """
    def __init__(self, identifier):
        super(ExtractedLicense, self).__init__(None, identifier)
        self.text = None
        self.cross_ref = []
        self.comment = None

    def __eq__(self, other):
        return (
            isinstance(other, ExtractedLicense)
            and self.identifier == other.identifier
            and self.full_name == other.full_name)

    def __lt__(self, other):
        return isinstance(other, ExtractedLicense) and self.identifier < other.identifier

    def add_xref(self, ref):
        self.cross_ref.append(ref)

    def validate(self, messages):
        if self.text is None:
            messages = messages + ['ExtractedLicense text can not be None']

        return messages


class Document(object):
    """
    Represent an SPDX document with these fields:
    - version: Spec version. Mandatory, one - Type: Version.
    - data_license: SPDX-Metadata license. Mandatory, one. Type: License.
    - name: Name of the document. Mandatory, one. Type: str.
    - spdx_id: SPDX Identifier for the document to refer to itself in
      relationship to other elements. Mandatory, one. Type: str.
    - ext_document_references: External SPDX documents referenced within the
        given SPDX document. Optional, one or many. Type: ExternalDocumentRef
    - comment: Comments on the SPDX file, optional one. Type: str
    - namespace: SPDX document specific namespace. Mandatory, one. Type: str
    - creation_info: SPDX file creation info. Mandatory, one. Type: CreationInfo
    - package: Package described by this document. Mandatory, one. Type: Package
    - extracted_licenses: List of licenses extracted that are not part of the
      SPDX license list. Optional, many. Type: ExtractedLicense.
    - reviews: SPDX document review information, Optional zero or more.
      Type: Review.
    - annotations: SPDX document annotation information, Optional zero or more.
      Type: Annotation.
    - snippet: Snippet information. Optional zero or more. Type: Snippet.
    """

    def __init__(self, version=None, data_license=None, name=None, spdx_id=None,
                 namespace=None, comment=None, package=None):
        # avoid recursive impor
        from spdx.creationinfo import CreationInfo
        self.version = version
        self.data_license = data_license
        self.name = name
        self.spdx_id = spdx_id
        self.ext_document_references = []
        self.comment = comment
        self.namespace = namespace
        self.creation_info = CreationInfo()
        self.package = package
        self.extracted_licenses = []
        self.reviews = []
        self.annotations = []
        self.snippet = []

    def add_review(self, review):
        self.reviews.append(review)

    def add_annotation(self, annotation):
        self.annotations.append(annotation)

    def add_extr_lic(self, lic):
        self.extracted_licenses.append(lic)

    def add_ext_document_reference(self, ext_doc_ref):
        self.ext_document_references.append(ext_doc_ref)

    def add_snippet(self, snip):
        self.snippet.append(snip)

    @property
    def files(self):
        return self.package.files

    @files.setter
    def files(self, value):
        self.package.files = value

    @property
    def has_comment(self):
        return self.comment is not None

    def validate(self, messages):
        """
        Validate all fields of the document and update the
        messages list with user friendly error messages for display.
        """
        messages = self.validate_version(messages)
        messages = self.validate_data_lics(messages)
        messages = self.validate_name(messages)
        messages = self.validate_spdx_id(messages)
        messages = self.validate_namespace(messages)
        messages = self.validate_ext_document_references(messages)
        messages = self.validate_creation_info(messages)
        messages = self.validate_package(messages)
        messages = self.validate_extracted_licenses(messages)
        messages = self.validate_reviews(messages)
        messages = self.validate_snippet(messages)

        return messages

    def validate_version(self, messages):
        if self.version is None:
            messages = messages + ['Document has no version.']

        return messages

    def validate_data_lics(self, messages):
        if self.data_license is None:
            messages = messages + ['Document has no data license.']
        else:
        # FIXME: REALLY? what if someone wants to use something else?
            if self.data_license.identifier != 'CC0-1.0':
                messages = messages + ['Document data license must be CC0-1.0.']

        return messages

    def validate_name(self, messages):
        if self.name is None:
            messages = messages + ['Document has no name.']

        return messages

    def validate_namespace(self, messages):
        if self.namespace is None:
            messages = messages + ['Document has no namespace.']

        return messages

    def validate_spdx_id(self, messages):
        if self.spdx_id is None:
            messages = messages + ['Document has no SPDX Identifier.']
        else:
            if not self.spdx_id.endswith('SPDXRef-DOCUMENT'):
                messages = messages + [
                    'Invalid Document SPDX Identifier value.'
                ]

        return messages

    def validate_ext_document_references(self, messages):
        for doc in self.ext_document_references:
            if isinstance(doc, ExternalDocumentRef):
                messages = doc.validate(messages)
            else:
                messages = list(messages) + [
                    'External document references must be of the type '
                    'spdx.document.ExternalDocumentRef and not ' + str(type(doc))
                ]
        return messages

    def validate_reviews(self, messages):
        for review in self.reviews:
            messages = review.validate(messages)

        return messages

    def validate_annotations(self, messages):
        for annotation in self.annotations:
            messages = annotation.validate(messages)

        return messages

    def validate_snippet(self, messages=None):
        for snippet in self.snippet:
            messages = snippet.validate(messages)

        return messages

    def validate_creation_info(self, messages):
        if self.creation_info is not None:
            messages = self.creation_info.validate(messages)
        else:
            messages = messages + ['Document has no creation information.']

        return messages

    def validate_package(self, messages):
        if self.package is not None:
            messages = self.package.validate(messages)
        else:
            messages = messages + ['Document has no package.']

        return messages

    def validate_extracted_licenses(self, messages):
        for lic in self.extracted_licenses:
            if isinstance(lic, ExtractedLicense):
                messages = lic.validate(messages)
            else:
                messages = messages + [
                    'Document extracted licenses must be of type '
                    'spdx.document.ExtractedLicense and not ' + type(lic)
                ]
        return messages
