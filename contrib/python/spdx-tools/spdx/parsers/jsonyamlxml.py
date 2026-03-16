
# Copyright (c) Xavier Figueroa
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

from spdx import document
from spdx.document import LicenseConjunction
from spdx.document import LicenseDisjunction
from spdx.parsers.builderexceptions import SPDXValueError, CardinalityError, OrderError
from spdx.parsers import rdf
from spdx import utils
from spdx.utils import UnKnown


ERROR_MESSAGES = rdf.ERROR_MESSAGES


class BaseParser(object):
    def __init__(self, builder, logger):
        self.builder = builder
        self.logger = logger

    def order_error(self, first_tag, second_tag):
        """
        Helper method for logging an OrderError raised.
        - first_tag: field to be added
        - second_tag: required field
        """
        self.error = True
        msg = '{0} Can not appear before {1}'.format(first_tag, second_tag)
        self.logger.log(msg)

    def more_than_one_error(self, field):
        """
        Helper method for logging an CardinalityError raised.
        - field: field/property that has been already defined.
        """
        msg = 'More than one {0} defined.'.format(field)
        self.logger.log(msg)
        self.error = True

    def value_error(self, key, bad_value):
        """
        Helper method for logging an SPDXValueError raised.
        It reports a value error using ERROR_MESSAGES dict.
        - key: key to use for ERROR_MESSAGES. If not present, a default message is logged
        - bad_value: malformed value
        """
        msg = ERROR_MESSAGES.get(key)
        if msg:
            self.logger.log(msg.format(bad_value))
        else:
            msg = "'{0}' is not a valid value for {1}".format(bad_value, key)
            self.logger.log(msg)
        self.error = True

class CreationInfoParser(BaseParser):
    def __init__(self, builder, logger):
        super(CreationInfoParser, self).__init__(builder, logger)

    def parse_creation_info(self, creation_info):
        """
        Parse Creation Information fields
        - creation_info: Python dict with Creation Information fields in it
        """
        if isinstance(creation_info, dict):
            self.parse_creation_info_comment(creation_info.get('comment'))
            self.parse_creation_info_lic_list_version(creation_info.get('licenseListVersion'))
            self.parse_creation_info_created(creation_info.get('created'))
            self.parse_creation_info_creators(creation_info.get('creators'))
        else:
            self.value_error('CREATION_INFO_SECTION', creation_info)

    def parse_creation_info_comment(self, comment):
        """
        Parse CreationInfo comment
        - comment: Python str/unicode
        """
        if isinstance(comment, six.string_types):
            try:
                return self.builder.set_creation_comment(self.document, comment)
            except CardinalityError:
                self.more_than_one_error('CreationInfo comment')
        elif comment is not None:
            self.value_error('CREATION_COMMENT', comment)

    def parse_creation_info_lic_list_version(self, license_list_version):
        """
        Parse CreationInfo license list version
        - license_list_version: Python str/unicode
        """
        if isinstance(license_list_version, six.string_types):
            try:
                return self.builder.set_lics_list_ver(self.document, license_list_version)
            except SPDXValueError:
                raise
                self.value_error('LL_VALUE', license_list_version)
            except CardinalityError:
                self.more_than_one_error('CreationInfo licenseListVersion')
        elif license_list_version is not None:
            self.value_error('LL_VALUE', license_list_version)

    def parse_creation_info_created(self, created):
        """
        Parse CreationInfo creation date
        - created: Python str/unicode (ISO-8601 representation of datetime)
        """
        if isinstance(created, six.string_types):
            try:
                return self.builder.set_created_date(self.document, created)
            except SPDXValueError:
                self.value_error('CREATED_VALUE', created)
            except CardinalityError:
                self.more_than_one_error('CreationInfo created')
        else:
            self.value_error('CREATED_VALUE', created)

    def parse_creation_info_creators(self, creators):
        """
        Parse CreationInfo creators
        - creators: Python list of creators (str/unicode)
        """
        if isinstance(creators, list):
            for creator in creators:
                if isinstance(creator, six.string_types):
                    entity = self.builder.create_entity(self.document, creator)
                    try:
                        self.builder.add_creator(self.document, entity)
                    except SPDXValueError:
                        self.value_error('CREATOR_VALUE', creator)
                else:
                    self.value_error('CREATOR_VALUE', creator)
        else:
            self.value_error('CREATORS_SECTION', creators)

class  ExternalDocumentRefsParser(BaseParser):
    def __init__(self, builder, logger):
        super(ExternalDocumentRefsParser, self).__init__(builder, logger)

    def parse_external_document_refs(self, external_document_refs):
        """
        Parse External Document References fields
        - external_document_refs: Python list with External Document References dicts in it
        """
        if isinstance(external_document_refs, list):
            for external_document_ref in external_document_refs:
                if isinstance(external_document_ref, dict):
                    self.parse_ext_doc_ref_id(external_document_ref.get('externalDocumentId'))
                    self.parse_ext_doc_ref_namespace(external_document_ref.get('spdxDocumentNamespace'))
                    self.parse_ext_doc_ref_chksum(external_document_ref.get('checksum'))
                else:
                    self.value_error('EXT_DOC_REF', external_document_ref)
        elif external_document_refs is not None:
            self.value_error('EXT_DOC_REFS_SECTION', external_document_refs)

    def parse_ext_doc_ref_id(self, ext_doc_ref_id):
        """
        Parse ExternalDocumentReference id
        ext_doc_ref_id: Python str/unicode
        """
        if isinstance(ext_doc_ref_id, six.string_types):
            return self.builder.set_ext_doc_id(self.document, ext_doc_ref_id)
        self.value_error('EXT_DOC_REF_ID', ext_doc_ref_id)
        return self.builder.set_ext_doc_id(self.document, 'dummy_ext_doc_ref')
        # ext_doc_ref_id is set even if it is None or not string. If weren't, the other attributes
        # would be added to the ex_doc_ref previously added.
        # Another approach is to skip the whole ex_doc_ref itself

    def parse_ext_doc_ref_namespace(self, namespace):
        """
        Parse ExternalDocumentReference namespace
        namespace: Python str/unicode
        """
        if isinstance(namespace, six.string_types):
            try:
                return self.builder.set_spdx_doc_uri(self.document, namespace)
            except SPDXValueError:
                self.value_error('EXT_DOC_REF_VALUE', namespace)
        else:
            self.value_error('EXT_DOC_REF_VALUE', namespace)

    def parse_ext_doc_ref_chksum(self, chksum):
        """
        Parse ExternalDocumentReference checksum
        chksum: Python dict('algorithm':str/unicode, 'value':str/unicode)
        """
        if isinstance(chksum, dict):
            value = chksum.get('value')
            if isinstance(value, six.string_types):
                try:
                    return self.builder.set_chksum(self.document, value)
                except SPDXValueError:
                    self.value_error('CHECKSUM_VALUE', value)
            else:
                self.value_error('CHECKSUM_VALUE', value)
        else:
            self.value_error('CHECKSUM_FIELD', chksum)

class LicenseParser(BaseParser):

    def __init__(self, builder, logger):
        super(LicenseParser, self).__init__(builder, logger)

    def parse_extracted_license_info(self, extracted_license_info):
        """
        Parse Extracted Lisence Information fields
        - extracted_license_info: Python list with Extracted Lisence Information dicts in it
        """
        if isinstance(extracted_license_info, list):
            for extracted_license in extracted_license_info:
                if isinstance(extracted_license, dict):
                    if self.parse_ext_lic_id(extracted_license.get('licenseId')):
                        self.parse_ext_lic_name(extracted_license.get('name'))
                        self.parse_ext_lic_comment(extracted_license.get('comment'))
                        self.parse_ext_lic_text(extracted_license.get('extractedText'))
                        self.parse_ext_lic_cross_refs(extracted_license.get('seeAlso'))
                else:
                    self.value_error('EXTR_LIC', extracted_license)

    def parse_ext_lic_id(self, ext_lic_id):
        """
        Parse ExtractedLicenseInformation id
        ext_lic_id: Python str/unicode
        """
        if isinstance(ext_lic_id, six.string_types):
            try:
                return self.builder.set_lic_id(self.document, ext_lic_id)
            except SPDXValueError:
                self.value_error('EXTR_LIC_ID', ext_lic_id)
        else:
            self.value_error('EXTR_LIC_ID', ext_lic_id)

    def parse_ext_lic_name(self, ext_lic_name):
        """
        Parse ExtractedLicenseInformation name
        ext_lic_name: Python str/unicode
        """
        try:
            return self.builder.set_lic_name(self.document, ext_lic_name)
        except SPDXValueError:
            self.value_error('EXTR_LIC_NAME', ext_lic_name)
        except CardinalityError:
            self.more_than_one_error('ExtractedLicense name')
        except OrderError:
            self.order_error('ExtractedLicense name', 'ExtractedLicense id')

    def parse_ext_lic_comment(self, ext_lic_comment):
        """
        Parse ExtractedLicenseInformation comment
        ext_lic_comment: Python str/unicode
        """
        if isinstance(ext_lic_comment, six.string_types):
            try:
                return self.builder.set_lic_comment(self.document, ext_lic_comment)
            except CardinalityError:
                self.more_than_one_error('ExtractedLicense comment')
            except OrderError:
                self.order_error('ExtractedLicense comment', 'ExtractedLicense id')
        elif ext_lic_comment is not None:
            self.value_error('EXTR_LIC_COMMENT', ext_lic_comment)

    def parse_ext_lic_text(self, ext_lic_text):
        """
        Parse ExtractedLicenseInformation text
        ext_lic_text: Python str/unicode
        """
        if isinstance(ext_lic_text, six.string_types):
            try:
                return self.builder.set_lic_text(self.document, ext_lic_text)
            except CardinalityError:
                self.more_than_one_error('ExtractedLicense text')
            except OrderError:
                self.order_error('ExtractedLicense text', 'ExtractedLicense id')
        else:
            self.value_error('EXTR_LIC_TXT', ext_lic_text)

    def parse_ext_lic_cross_refs(self, cross_refs):
        """
        Parse ExtractedLicenseInformation cross references
        cross_refs: Python list of cross references (str/unicode)
        """
        if isinstance(cross_refs, list):
            for cross_ref in cross_refs:
                if isinstance(cross_ref, six.string_types):
                    try:
                        self.builder.add_lic_xref(self.document, cross_ref)
                    except OrderError:
                        self.order_error('ExtractedLicense cross references', 'ExtractedLicense id')
                else:
                    self.value_error('CROSS_REF', cross_ref)

    def replace_license(self, license_object):
        if isinstance(license_object, LicenseConjunction):
            return LicenseConjunction(self.replace_license(license_object.license_1), self.replace_license(license_object.license_2))
        elif isinstance(license_object, LicenseDisjunction):
            return LicenseDisjunction(self.replace_license(license_object.license_1), self.replace_license(license_object.license_2))
        else:
            license_objects = list(filter(lambda lic: lic.identifier == license_object.identifier, self.document.extracted_licenses))
            return license_objects[-1] if license_objects else license_object

class AnnotationParser(BaseParser):
    def __init__(self, builder, logger):
        super(AnnotationParser, self).__init__(builder, logger)

    def parse_annotations(self, annotations):
        """
        Parse Annotation Information fields
        - annotations: Python list with Annotation Information dicts in it
        """
        if isinstance(annotations, list):
            for annotation in annotations:
                if isinstance(annotation, dict):
                    if self.parse_annotation_annotator(annotation.get('annotator')):
                        self.parse_annotation_date(annotation.get('annotationDate'))
                        self.parse_annotation_comment(annotation.get('comment'))
                        self.parse_annotation_type(annotation.get('annotationType'))
                        self.parse_annotation_id(annotation.get('id'))
                else:
                    self.value_error('ANNOTATION', annotation)

    def parse_annotation_annotator(self, annotator):
        """
        Parse Annotation annotator
        - annotator: Python str/unicode
        """
        if isinstance(annotator, six.string_types):
            entity = self.builder.create_entity(self.document, annotator)
            try:
                return self.builder.add_annotator(self.document, entity)
            except SPDXValueError:
                self.value_error('ANNOTATOR_VALUE', annotator)
        else:
            self.value_error('ANNOTATOR_VALUE', annotator)


    def parse_annotation_date(self, date):
        """
        Parse Annotation date
        - date: Python str/unicode (ISO-8601 representation of datetime)
        """
        if isinstance(date, six.string_types):
            try:
                return self.builder.add_annotation_date(self.document, date)
            except SPDXValueError:
                self.value_error('ANNOTATION_DATE', date)
            except CardinalityError:
                self.more_than_one_error('Annotation date')
            except OrderError:
                self.order_error('ANNOTATION_DATE', 'ANNOTATOR_VALUE')
        else:
            self.value_error('ANNOTATION_DATE', date)

    def parse_annotation_comment(self, comment):
        """
        Parse Annotation comment
        - comment: Python str/unicode
        """
        if isinstance(comment, six.string_types):
            try:
                return self.builder.add_annotation_comment(self.document, comment)
            except CardinalityError:
                self.more_than_one_error('Annotation comment')
            except OrderError:
                self.order_error('ANNOTATION_COMMENT', 'ANNOTATOR_VALUE')
        else:
            self.value_error('ANNOTATION_COMMENT', comment)

    def parse_annotation_type(self, annotation_type):
        """
        Parse Annotation type
        - annotation_type: Python str/unicode (REVIEW or OTHER)
        """
        if isinstance(annotation_type, six.string_types):
            try:
                return self.builder.add_annotation_type(self.document, annotation_type)
            except SPDXValueError:
                self.value_error('ANNOTATION_TYPE', annotation_type)
            except CardinalityError:
                self.more_than_one_error('ANNOTATION_TYPE')
            except OrderError:
                self.order_error('ANNOTATION_TYPE', 'ANNOTATOR_VALUE')
        else:
            self.value_error('ANNOTATION_TYPE', annotation_type)

    def parse_annotation_id(self, annotation_id):
        """
        Parse Annotation id
        - annotation_id: Python str/unicode
        """
        if isinstance(annotation_id, six.string_types):
            try:
                return self.builder.set_annotation_spdx_id(self.document, annotation_id)
            except CardinalityError:
                self.more_than_one_error('ANNOTATION_ID')
            except OrderError:
                self.order_error('ANNOTATION_ID', 'ANNOTATOR_VALUE')
        else:
            self.value_error('ANNOTATION_ID', annotation_id)

class SnippetParser(BaseParser):
    def __init__(self, builder, logger):
        super(SnippetParser, self).__init__(builder, logger)

    def parse_snippets(self, snippets):
        """
        Parse Snippet Information fields
        - snippets: Python list with Snippet Information dicts in it
        """
        if isinstance(snippets, list):
            for snippet in snippets:
                if isinstance(snippet, dict):
                    if self.parse_snippet_id(snippet.get('id')):
                        self.parse_snippet_name(snippet.get('name'))
                        self.parse_snippet_comment(snippet.get('comment'))
                        self.parse_snippet_copyright(snippet.get('copyrightText'))
                        self.parse_snippet_license_comment(snippet.get('licenseComments'))
                        self.parse_snippet_file_spdxid(snippet.get('fileId'))
                        self.parse_snippet_concluded_license(snippet.get('licenseConcluded'))
                        self.parse_snippet_license_info_from_snippet(snippet.get('licenseInfoFromSnippet'))
                else:
                    self.value_error('SNIPPET', snippet)

    def parse_snippet_id(self, snippet_id):
        """
        Parse Snippet id
        - snippet_id: Python str/unicode
        """
        if isinstance(snippet_id, six.string_types):
            try:
                return self.builder.create_snippet(self.document, snippet_id)
            except SPDXValueError:
                self.value_error('SNIPPET_SPDX_ID_VALUE', snippet_id)
        else:
            self.value_error('SNIPPET_SPDX_ID_VALUE', snippet_id)

    def parse_snippet_name(self, snippet_name):
        """
        Parse Snippet name
        - snippet_name: Python str/unicode
        """
        if isinstance(snippet_name, six.string_types):
            try:
                return self.builder.set_snippet_name(self.document, snippet_name)
            except CardinalityError:
                self.more_than_one_error('SNIPPET_NAME')
        elif snippet_name is not None:
            self.value_error('SNIPPET_NAME', snippet_name)

    def parse_snippet_comment(self, snippet_comment):
        """
        Parse Snippet comment
        - snippet_comment: Python str/unicode
        """
        if isinstance(snippet_comment, six.string_types):
            try:
                return self.builder.set_snippet_comment(self.document, snippet_comment)
            except CardinalityError:
                self.more_than_one_error('SNIPPET_COMMENT')
        elif snippet_comment is not None:
            self.value_error('SNIPPET_COMMENT', snippet_comment)

    def parse_snippet_copyright(self, copyright_text):
        """
        Parse Snippet copyright text
        - copyright_text: Python str/unicode
        """
        if isinstance(copyright_text, six.string_types):
            try:
                return self.builder.set_snippet_copyright(self.document, copyright_text)
            except CardinalityError:
                self.more_than_one_error('SNIPPET_COPYRIGHT')
        else:
            self.value_error('SNIPPET_COPYRIGHT', copyright_text)

    def parse_snippet_license_comment(self, license_comment):
        """
        Parse Snippet license comment
        - license_comment: Python str/unicode
        """
        if isinstance(license_comment, six.string_types):
            try:
                return self.builder.set_snippet_lic_comment(self.document, license_comment)
            except CardinalityError:
                self.more_than_one_error('SNIPPET_LIC_COMMENTS')
        elif license_comment is not None:
            self.value_error('SNIPPET_LIC_COMMENTS', license_comment)

    def parse_snippet_file_spdxid(self, file_spdxid):
        """
        Parse Snippet file id
        - file_spdxid: Python str/unicode
        """
        if isinstance(file_spdxid, six.string_types):
            try:
                return self.builder.set_snip_from_file_spdxid(self.document, file_spdxid)
            except SPDXValueError:
                self.value_error('SNIPPET_FILE_ID', file_spdxid)
            except CardinalityError:
                self.more_than_one_error('SNIPPET_FILE_ID')
        else:
            self.value_error('SNIPPET_FILE_ID', file_spdxid)

    def parse_snippet_concluded_license(self, concluded_license):
        """
        Parse Snippet concluded license
        - concluded_license: Python str/unicode
        """
        if isinstance(concluded_license, six.string_types):
            lic_parser = utils.LicenseListParser()
            lic_parser.build(write_tables=0, debug=0)
            license_object = self.replace_license(lic_parser.parse(concluded_license))
            try:
                return self.builder.set_snip_concluded_license(self.document, license_object)
            except SPDXValueError:
                self.value_error('SNIPPET_SINGLE_LICS', concluded_license)
            except CardinalityError:
                self.more_than_one_error('SNIPPET_SINGLE_LICS')
        else:
            self.value_error('SNIPPET_SINGLE_LICS', concluded_license)

    def parse_snippet_license_info_from_snippet(self, license_info_from_snippet):
        """
        Parse Snippet license information from snippet
        - license_info_from_snippet: Python list of licenses information from snippet (str/unicode)
        """
        if isinstance(license_info_from_snippet, list):
            for lic_in_snippet in license_info_from_snippet:
                if isinstance(lic_in_snippet, six.string_types):
                    lic_parser = utils.LicenseListParser()
                    lic_parser.build(write_tables=0, debug=0)
                    license_object = self.replace_license(lic_parser.parse(lic_in_snippet))
                    try:
                        return self.builder.set_snippet_lics_info(self.document, license_object)
                    except SPDXValueError:
                        self.value_error('SNIPPET_LIC_INFO', lic_in_snippet)
                else:
                    self.value_error('SNIPPET_LIC_INFO', lic_in_snippet)
        else:
            self.value_error('SNIPPET_LIC_INFO_FIELD', license_info_from_snippet)

class ReviewParser(BaseParser):
    def __init__(self, builder, logger):
        super(ReviewParser, self).__init__(builder, logger)

    def parse_reviews(self, reviews):
        """
        Parse Review Information fields
        - reviews: Python list with Review Information dicts in it
        """
        if isinstance(reviews, list):
            for review in reviews:
                if isinstance(review, dict):
                    if self.parse_review_reviewer(review.get('reviewer')):
                        self.parse_review_date(review.get('reviewDate'))
                        self.parse_review_comment(review.get('comment'))
                else:
                    self.value_error('REVIEW', review)

    def parse_review_reviewer(self, reviewer):
        """
        Parse Review reviewer
        - reviewer: Python str/unicode
        """
        if isinstance(reviewer, six.string_types):
            entity = self.builder.create_entity(self.document, reviewer)
            try:
                return self.builder.add_reviewer(self.document, entity)
            except SPDXValueError:
                self.value_error('REVIEWER_VALUE', reviewer)
        else:
            self.value_error('REVIEWER_VALUE', reviewer)

    def parse_review_date(self, review_date):
        """
        Parse Review date
        - review_date: Python str/unicode (ISO-8601 representation of datetime)
        """
        if isinstance(review_date, six.string_types):
            try:
                return self.builder.add_review_date(self.document, review_date)
            except SPDXValueError:
                self.value_error('REVIEW_DATE', review_date)
            except CardinalityError:
                self.more_than_one_error('REVIEW_DATE')
            except OrderError:
                self.order_error('REVIEW_DATE', 'REVIEWER_VALUE')
        else:
            self.value_error('REVIEW_DATE', review_date)

    def parse_review_comment(self, review_comment):
        """
        Parse Review comment
        - review_comment: Python str/unicode
        """
        if isinstance(review_comment, six.string_types):
            try:
                return self.builder.add_review_comment(self.document, review_comment)
            except CardinalityError:
                self.more_than_one_error('REVIEW_COMMENT')
            except OrderError:
                self.order_error('REVIEW_COMMENT', 'REVIEWER_VALUE')
        elif review_comment is not None:
            self.value_error('REVIEW_COMMENT', review_comment)

class FileParser(BaseParser):
    def __init__(self, builder, logger):
        super(FileParser, self).__init__(builder, logger)

    def parse_file(self, file):
        """
        Parse File Information fields
        - file: Python dict with File Information fields in it
        """
        if isinstance(file, dict):
            self.parse_file_name(file.get('name'))
            self.parse_file_id(file.get('id'))
            self.parse_file_types(file.get('fileTypes'))
            self.parse_file_concluded_license(file.get('licenseConcluded'))
            self.parse_file_license_info_from_files(file.get('licenseInfoFromFiles'))
            self.parse_file_license_comments(file.get('licenseComments'))
            self.parse_file_copyright_text(file.get('copyrightText'))
            self.parse_file_artifacts(file.get('artifactOf'))
            self.parse_file_comment(file.get('comment'))
            self.parse_file_notice_text(file.get('noticeText'))
            self.parse_file_contributors(file.get('fileContributors'))
            self.parse_file_dependencies(file.get('fileDependencies'))
            self.parse_annotations(file.get('annotations'))
            self.parse_file_chksum(file.get('sha1'))
        else:
            self.value_error('FILE', file)

    def parse_file_name(self, file_name):
        """
        Parse File name
        - file_name: Python str/unicode
        """
        if isinstance(file_name, six.string_types):
            return self.builder.set_file_name(self.document, file_name)
        self.value_error('FILE_NAME', file_name)
        return self.builder.set_file_name(self.document, 'dummy_file')
        # file_name is set even if it is None or not string. If weren't, the other attributes
        # would be added to the file previously added.
        # Another approach is to skip the whole file itself

    def parse_file_id(self, file_id):
        """
        Parse File id
        - file_id: Python str/unicode
        """
        if isinstance(file_id, six.string_types):
            try:
                return self.builder.set_file_spdx_id(self.document, file_id)
            except SPDXValueError:
                self.value_error('FILE_ID', file_id)
            except CardinalityError:
                self.more_than_one_error('FILE_ID')
            except OrderError:
                self.order_error('FILE_ID', 'FILE_NAME')
        else:
            self.value_error('FILE_ID', file_id)

    def parse_file_types(self, file_types):
        """
        Parse File types
        - file_types: Python list of file types (str/unicode: fileType_archive, fileType_binary, fileType_source or fileType_other)
        """
        if isinstance(file_types, list):  # file_types is an array in JSON examples...
            for file_type in file_types:
                self.parse_file_type(file_type)
        # ...but file.File allows only one type at the moment.
        elif isinstance(file_types, six.string_types):
            return self.parse_file_type(file_types)
        elif file_types is not None:
            self.value_error('FILE_TYPES', file_types)

    def parse_file_type(self, file_type):
        """
        Parse File type
        - file_type: Python str/unicode (fileType_archive, fileType_binary, fileType_source or fileType_other)
        """
        if isinstance(file_type, six.string_types):
            try:
                return self.builder.set_file_type(self.document, file_type)
            except SPDXValueError:
                self.value_error('FILE_TYPE', file_type)
            except CardinalityError:
                self.more_than_one_error('FILE_TYPE')
            except OrderError:
                self.order_error('FILE_TYPE', 'FILE_NAME')
        else:
            self.value_error('FILE_TYPE', file_type)

    def parse_file_concluded_license(self, concluded_license):
        """
        Parse File concluded license
        - concluded_license: Python str/unicode
        """
        if isinstance(concluded_license, six.string_types):
            lic_parser = utils.LicenseListParser()
            lic_parser.build(write_tables=0, debug=0)
            license_object = self.replace_license(lic_parser.parse(concluded_license))
            try:
                return self.builder.set_concluded_license(self.document, license_object)
            except SPDXValueError:
                self.value_error('FILE_SINGLE_LICS', concluded_license)
            except CardinalityError:
                self.more_than_one_error('FILE_SINGLE_LICS')
            except OrderError:
                self.order_error('FILE_SINGLE_LICS', 'FILE_NAME')
        else:
            self.value_error('FILE_SINGLE_LICS', concluded_license)

    def parse_file_license_info_from_files(self, license_info_from_files):
        """
        Parse File license information from files
        - license_info_from_files: Python list of licenses information from files (str/unicode)
        """
        if isinstance(license_info_from_files, list):
            for license_info_from_file in license_info_from_files:
                if isinstance(license_info_from_file, six.string_types):
                    lic_parser = utils.LicenseListParser()
                    lic_parser.build(write_tables=0, debug=0)
                    license_object = self.replace_license(lic_parser.parse(license_info_from_file))
                    try:
                        self.builder.set_file_license_in_file(self.document, license_object)
                    except SPDXValueError:
                        self.value_error('FILE_LIC_FRM_FILES', license_info_from_file)
                    except OrderError:
                        self.order_error('FILE_LIC_FRM_FILES', 'FILE_NAME')
                else:
                    self.value_error('FILE_LIC_FRM_FILES', license_info_from_file)
        else:
            self.value_error('FILE_LIC_FRM_FILES_FIELD', license_info_from_files)

    def parse_file_license_comments(self, license_comments):
        """
        Parse File license comments
        - license_comments: Python str/unicode
        """
        if isinstance(license_comments, six.string_types):
            try:
                return self.builder.set_file_license_comment(self.document, license_comments)
            except CardinalityError:
                self.more_than_one_error('FILE_LIC_COMMENTS')
            except OrderError:
                self.order_error('FILE_LIC_COMMENTS', 'FILE_NAME')
        elif license_comments is not None:
            self.value_error('FILE_LIC_COMMENTS', license_comments)

    def parse_file_copyright_text(self, copyright_text):
        """
        Parse File copyright text
        - copyright_text: Python str/unicode
        """
        if isinstance(copyright_text, six.string_types):
            try:
                return self.builder.set_file_copyright(self.document, copyright_text)
            except CardinalityError:
                self.more_than_one_error('FILE_COPYRIGHT_TEXT')
            except OrderError:
                self.order_error('FILE_COPYRIGHT_TEXT', 'FILE_NAME')
        else:
            self.value_error('FILE_COPYRIGHT_TEXT', copyright_text)

    def parse_file_artifacts(self, file_artifacts):
        """
        Parse File artifacts
        - file_artifacts: Python list of dict('name':str/unicode, 'homePage':str/unicode, 'projectUri':str/unicode)
        """
        if isinstance(file_artifacts, list):
            for artifact in file_artifacts:
                if isinstance(artifact, dict):
                    self.builder.set_file_atrificat_of_project(self.document, 'name', artifact.get('name', UnKnown()))
                    self.builder.set_file_atrificat_of_project(self.document, 'home', artifact.get('homePage', UnKnown()))
                    self.builder.set_file_atrificat_of_project(self.document, 'uri', artifact.get('projectUri', UnKnown()))
                    return True
                else:
                    self.value_error('ARTIFACT_OF_VALUE', artifact)
        elif file_artifacts is not None:
            self.value_error('ARTIFACT_OF_FIELD', file_artifacts)

    def parse_file_comment(self, file_comment):
        """
        Parse File comment
        - file_comment: Python str/unicode
        """
        if isinstance(file_comment, six.string_types):
            try:
                return self.builder.set_file_comment(self.document, file_comment)
            except CardinalityError:
                self.more_than_one_error('FILE_COMMENT')
            except OrderError:
                self.order_error('FILE_COMMENT', 'FILE_NAME')
        elif file_comment is not None:
            self.value_error('FILE_COMMENT', file_comment)

    def parse_file_notice_text(self, notice_text):
        """
        Parse File notice text
        - notice_text: Python str/unicode
        """
        if isinstance(notice_text, six.string_types):
            try:
                return self.builder.set_file_notice(self.document, notice_text)
            except CardinalityError:
                self.more_than_one_error('FILE_NOTICE_TEXT')
            except OrderError:
                self.order_error('FILE_NOTICE_TEXT', 'FILE_NAME')
        elif notice_text is not None:
            self.value_error('FILE_NOTICE_TEXT', notice_text)

    def parse_file_contributors(self, file_contributors):
        """
        Parse File contributors
        - file_contributors: Python list of contributors (str/unicode)
        """
        if isinstance(file_contributors, list):
            for contributor in file_contributors:
                if isinstance(contributor, six.string_types):
                    try:
                        self.builder.add_file_contribution(self.document, contributor)
                    except OrderError:
                        self.order_error('FILE_CONTRIBUTOR', 'FILE_NAME')
                else:
                    self.value_error('FILE_CONTRIBUTOR', contributor)
        elif file_contributors is not None:
            self.value_error('FILE_CONTRIBUTORS', file_contributors)

    def parse_file_dependencies(self, file_dependencies):
        """
        Parse File dependencies
        - file_dependencies: Python list of dependencies (str/unicode or file dict as in FileParser.parse_file)
        """
        if isinstance(file_dependencies, list):
            for dependency in file_dependencies:
                dependency = self._handle_file_dependency(dependency)
                if isinstance(dependency, six.string_types):
                    try:
                        self.builder.add_file_dep(self.document, dependency)
                    except OrderError:
                        self.order_error('FILE_DEPENDENCY', 'FILE_NAME')
                else:
                    self.value_error('FILE_DEPENDENCY', dependency)
        elif file_dependencies is not None:
            self.value_error('FILE_DEPENDENCIES', file_dependencies)

    def _handle_file_dependency(self, file_dependency):
        """
        Helper method that handles file-like dependency
        - file_dependency: Python dict as in FileParser.parse_file
        return: file name (str/unicode) or None
        """
        if isinstance(file_dependency, dict):
            filelike_dependency = file_dependency.get('File')
            if isinstance(filelike_dependency, dict):
                return filelike_dependency.get('name')
            return None
        return None

    def parse_file_chksum(self, file_chksum):
        """
        Parse File checksum
        - file_chksum: Python str/unicode
        """
        if isinstance(file_chksum, six.string_types):
            try:
                return self.builder.set_file_chksum(self.document, file_chksum)
            except CardinalityError:
                self.more_than_one_error('FILE_CHECKSUM')
            except OrderError:
                self.order_error('FILE_CHECKSUM', 'FILE_NAME')
        else:
            self.value_error('FILE_CHECKSUM', file_chksum)

class PackageParser(BaseParser):
    def __init__(self, builder, logger):
        super(PackageParser, self).__init__(builder, logger)

    def parse_package(self, package):
        """
        Parse Package Information fields
        - package: Python dict with Package Information fields in it
        """
        if isinstance(package, dict):
            self.parse_pkg_name(package.get('name'))
            self.parse_pkg_id(package.get('id'))
            self.parse_pkg_version(package.get('versionInfo'))
            self.parse_pkg_file_name(package.get('packageFileName'))
            self.parse_pkg_supplier(package.get('supplier'))
            self.parse_pkg_originator(package.get('originator'))
            self.parse_pkg_down_location(package.get('downloadLocation'))
            self.parse_pkg_verif_code_field(package.get('packageVerificationCode'))
            self.parse_pkg_homepage(package.get('homepage'))
            self.parse_pkg_source_info(package.get('sourceInfo'))
            self.parse_pkg_concluded_license(package.get('licenseConcluded'))
            self.parse_pkg_license_info_from_files(package.get('licenseInfoFromFiles'))
            self.parse_pkg_declared_license(package.get('licenseDeclared'))
            self.parse_pkg_license_comment(package.get('licenseComments'))
            self.parse_pkg_copyright_text(package.get('copyrightText'))
            self.parse_pkg_summary(package.get('summary'))
            self.parse_pkg_description(package.get('description'))
            self.parse_annotations(package.get('annotations'))
            self.parse_pkg_files(package.get('files'))
            self.parse_pkg_chksum(package.get('sha1'))
        else:
            self.value_error('PACKAGE', package)

    def parse_pkg_name(self, pkg_name):
        """
        Parse Package name
        - pkg_name: Python str/unicode
        """
        if isinstance(pkg_name, six.string_types):
            return self.builder.create_package(self.document, pkg_name)
        self.value_error('PKG_NAME', pkg_name)
        return self.builder.create_package(self.document, 'dummy_package')
        # pkg_name is set even if it is None or not string. If weren't, the other attributes
        # would be added to the package previously added.
        # Another approach is to skip the whole package itself

    def parse_pkg_id(self, pkg_id):
        """
        Parse Package id
        - pkg_id: Python str/unicode
        """
        if isinstance(pkg_id, six.string_types):
            try:
                return self.builder.set_pkg_spdx_id(self.document, pkg_id)
            except SPDXValueError:
                self.value_error('PKG_ID', pkg_id)
            except CardinalityError:
                self.more_than_one_error('PKG_ID')
        else:
            self.value_error('PKG_ID', pkg_id)

    def parse_pkg_version(self, pkg_version):
        """
        Parse Package version
        - pkg_name: Python str/unicode
        """
        if isinstance(pkg_version, six.string_types):
            try:
                return self.builder.set_pkg_vers(self.document, pkg_version)
            except CardinalityError:
                self.more_than_one_error('PKG_VERSION')
            except OrderError:
                self.order_error('PKG_VERSION', 'PKG_NAME')
        elif pkg_version is not None:
            self.value_error('PKG_VERSION', pkg_version)

    def parse_pkg_file_name(self, pkg_file_name):
        """
        Parse Package file name
        - pkg_file_name: Python str/unicode
        """
        if isinstance(pkg_file_name, six.string_types):
            try:
                return self.builder.set_pkg_file_name(self.document, pkg_file_name)
            except CardinalityError:
                self.more_than_one_error('PKG_FILE_NAME')
            except OrderError:
                self.order_error('PKG_FILE_NAME', 'PKG_NAME')
        elif pkg_file_name is not None:
            self.value_error('PKG_FILE_NAME', pkg_file_name)

    def parse_pkg_supplier(self, pkg_supplier):
        """
        Parse Package supplier
        - pkg_supplier: Python str/unicode
        """
        if isinstance(pkg_supplier, six.string_types):
            entity = self.builder.create_entity(self.document, pkg_supplier)
            try:
                return self.builder.set_pkg_supplier(self.document, entity)
            except SPDXValueError:
                self.value_error('PKG_SUPPL_VALUE', pkg_supplier)
            except CardinalityError:
                self.more_than_one_error('PKG_SUPPL_VALUE')
            except OrderError:
                self.order_error('PKG_SUPPL_VALUE', 'PKG_NAME')
        elif pkg_supplier is not None:
            self.value_error('PKG_SUPPL_VALUE', pkg_supplier)

    def parse_pkg_originator(self, pkg_originator):
        """
        Parse Package originator
        - pkg_originator: Python str/unicode
        """
        if isinstance(pkg_originator, six.string_types):
            entity = self.builder.create_entity(self.document, pkg_originator)
            try:
                return self.builder.set_pkg_originator(self.document, entity)
            except SPDXValueError:
                self.value_error('PKG_ORIGINATOR_VALUE', pkg_originator)
            except CardinalityError:
                self.more_than_one_error('PKG_ORIGINATOR_VALUE')
            except OrderError:
                self.order_error('PKG_ORIGINATOR_VALUE', 'PKG_NAME')
        elif pkg_originator is not None:
            self.value_error('PKG_ORIGINATOR_VALUE', pkg_originator)

    def parse_pkg_down_location(self, pkg_down_location):
        """
        Parse Package download location
        - pkg_down_location: Python str/unicode
        """
        if isinstance(pkg_down_location, six.string_types):
            try:
                return self.builder.set_pkg_down_location(self.document, pkg_down_location)
            except CardinalityError:
                self.more_than_one_error('PKG_DOWN_LOC')
            except OrderError:
                self.order_error('PKG_DOWN_LOC', 'PKG_NAME')
        else:
            self.value_error('PKG_DOWN_LOC', pkg_down_location)

    def parse_pkg_verif_code_field(self, pkg_verif_code_field):
        """
        Parse Package verification code dict
        - pkg_verif_code_field: Python dict('value':str/unicode, 'excludedFilesNames':list)
        """
        if isinstance(pkg_verif_code_field, dict):
            self.parse_pkg_verif_exc_files(pkg_verif_code_field.get('excludedFilesNames'))
            return self.parse_pkg_verif_code(pkg_verif_code_field.get('value'))
        else:
            self.value_error('PKG_VERIF_CODE_FIELD', pkg_verif_code_field)

    def parse_pkg_verif_code(self, pkg_verif_code):
        """
        Parse Package verification code value
        - pkg_verif_code: Python str/unicode
        """
        if isinstance(pkg_verif_code, six.string_types):
            try:
                return self.builder.set_pkg_verif_code(self.document, pkg_verif_code)
            except CardinalityError:
                self.more_than_one_error('PKG_VERIF_CODE')
            except OrderError:
                self.order_error('PKG_VERIF_CODE', 'PKG_NAME')
        else:
            self.value_error('PKG_VERIF_CODE', pkg_verif_code)

    def parse_pkg_verif_exc_files(self, pkg_verif_exc_files):
        """
        Parse Package files excluded from verification code
        - pkg_verif_exc_files: Python list of files excluded (str/unicode)
        """
        if isinstance(pkg_verif_exc_files, list):
            for pkg_verif_exc_file in pkg_verif_exc_files:
                if isinstance(pkg_verif_exc_file, six.string_types):
                    try:
                        self.builder.set_pkg_excl_file(self.document, pkg_verif_exc_file)
                    except OrderError:
                        self.order_error('PKG_VERIF_EXC_FILE', 'PKG_NAME')
                else:
                    self.value_error('PKG_VERIF_EXC_FILE', pkg_verif_exc_file)
        elif pkg_verif_exc_files is not None:
            self.value_error('PKG_VERIF_EXC_FILE_FIELD', pkg_verif_exc_files)

    def parse_pkg_homepage(self, pkg_homepage):
        """
        Parse Package homepage
        - pkg_homepage: Python str/unicode
        """
        if isinstance(pkg_homepage, six.string_types):
            try:
                return self.builder.set_pkg_home(self.document, pkg_homepage)
            except SPDXValueError:
                self.value_error('PKG_HOMEPAGE', pkg_homepage)
            except CardinalityError:
                self.more_than_one_error('PKG_HOMEPAGE')
            except OrderError:
                self.order_error('PKG_HOMEPAGE', 'PKG_NAME')
        elif pkg_homepage is not None:
            self.value_error('PKG_HOMEPAGE', pkg_homepage)

    def parse_pkg_source_info(self, pkg_source_info):
        """
        Parse Package source information
        - pkg_source_info: Python str/unicode
        """
        if isinstance(pkg_source_info, six.string_types):
            try:
                return self.builder.set_pkg_source_info(self.document, pkg_source_info)
            except CardinalityError:
                self.more_than_one_error('PKG_SRC_INFO')
            except OrderError:
                self.order_error('PKG_SRC_INFO', 'PKG_NAME')
        elif pkg_source_info is not None:
            self.value_error('PKG_SRC_INFO', pkg_source_info)

    def parse_pkg_concluded_license(self, pkg_concluded_license):
        """
        Parse Package concluded license
        - pkg_concluded_license: Python str/unicode
        """
        if isinstance(pkg_concluded_license, six.string_types):
            lic_parser = utils.LicenseListParser()
            lic_parser.build(write_tables=0, debug=0)
            license_object = self.replace_license(lic_parser.parse(pkg_concluded_license))
            try:
                return self.builder.set_pkg_licenses_concluded(self.document, license_object)
            except SPDXValueError:
                self.value_error('PKG_SINGLE_LICS', pkg_concluded_license)
            except CardinalityError:
                self.more_than_one_error('PKG_SINGLE_LICS')
            except OrderError:
                self.order_error('PKG_SINGLE_LICS', 'PKG_NAME')
        else:
            self.value_error('PKG_SINGLE_LICS', pkg_concluded_license)

    def parse_pkg_license_info_from_files(self, license_info_from_files):
        """
        Parse Package license information from files
        - license_info_from_files: Python list of licenses information from files (str/unicode)
        """
        if isinstance(license_info_from_files, list):
            for license_info_from_file in license_info_from_files:
                if isinstance(license_info_from_file, six.string_types):
                    lic_parser = utils.LicenseListParser()
                    lic_parser.build(write_tables=0, debug=0)
                    license_object = self.replace_license(lic_parser.parse(license_info_from_file))
                    try:
                        self.builder.set_pkg_license_from_file(self.document, license_object)
                    except SPDXValueError:
                        self.value_error('PKG_LIC_FRM_FILES', license_info_from_file)
                    except OrderError:
                        self.order_error('PKG_LIC_FRM_FILES', 'PKG_NAME')
                else:
                    self.value_error('PKG_LIC_FRM_FILES', license_info_from_file)
        else:
            self.value_error('PKG_LIC_FRM_FILES_FIELD', license_info_from_files)

    def parse_pkg_declared_license(self, pkg_declared_license):
        """
        Parse Package license declared
        - pkg_declared_license: Python str/unicode
        """
        if isinstance(pkg_declared_license, six.string_types):
            lic_parser = utils.LicenseListParser()
            lic_parser.build(write_tables=0, debug=0)
            license_object = self.replace_license(lic_parser.parse(pkg_declared_license))
            try:
                return self.builder.set_pkg_license_declared(self.document, license_object)
            except SPDXValueError:
                self.value_error('PKG_DECL_LIC', pkg_declared_license)
            except CardinalityError:
                self.more_than_one_error('PKG_DECL_LIC')
            except OrderError:
                self.order_error('PKG_DECL_LIC', 'PKG_NAME')
        else:
            self.value_error('PKG_DECL_LIC', pkg_declared_license)

    def parse_pkg_license_comment(self, pkg_license_comment):
        """
        Parse Package license comment
        - pkg_license_comment: Python str/unicode
        """
        if isinstance(pkg_license_comment, six.string_types):
            try:
                return self.builder.set_pkg_license_comment(self.document, pkg_license_comment)
            except CardinalityError:
                self.more_than_one_error('PKG_LIC_COMMENT')
            except OrderError:
                self.order_error('PKG_LIC_COMMENT', 'PKG_NAME')
        elif pkg_license_comment is not None:
            self.value_error('PKG_LIC_COMMENT', pkg_license_comment)

    def parse_pkg_copyright_text(self, pkg_copyright_text):
        """
        Parse Package copyright text
        - pkg_copyright_text: Python str/unicode
        """
        if isinstance(pkg_copyright_text, six.string_types):
            try:
                return self.builder.set_pkg_cr_text(self.document, pkg_copyright_text)
            except CardinalityError:
                self.more_than_one_error('PKG_COPYRIGHT_TEXT')
            except OrderError:
                self.order_error('PKG_COPYRIGHT_TEXT', 'PKG_NAME')
        else:
            self.value_error('PKG_COPYRIGHT_TEXT', pkg_copyright_text)

    def parse_pkg_summary(self, pkg_summary):
        """
        Parse Package summary
        - pkg_summary: Python str/unicode
        """
        if isinstance(pkg_summary, six.string_types):
            try:
                return self.builder.set_pkg_summary(self.document, pkg_summary)
            except CardinalityError:
                self.more_than_one_error('PKG_SUMMARY')
            except OrderError:
                self.order_error('PKG_SUMMARY', 'PKG_NAME')
        elif pkg_summary is not None:
            self.value_error('PKG_SUMMARY', pkg_summary)

    def parse_pkg_description(self, pkg_description):
        """
        Parse Package description
        - pkg_description: Python str/unicode
        """
        if isinstance(pkg_description, six.string_types):
            try:
                return self.builder.set_pkg_desc(self.document, pkg_description)
            except CardinalityError:
                self.more_than_one_error('PKG_DESCRIPTION')
            except OrderError:
                self.order_error('PKG_DESCRIPTION', 'PKG_NAME')
        elif pkg_description is not None:
            self.value_error('PKG_DESCRIPTION', pkg_description)

    def parse_pkg_files(self, pkg_files):
        """
        Parse Package files
        - pkg_files: Python list of dicts as in FileParser.parse_file
        """
        if isinstance(pkg_files, list):
            for pkg_file in pkg_files:
                if isinstance(pkg_file, dict):
                    self.parse_file(pkg_file.get('File'))
                else:
                    self.value_error('PKG_FILE', pkg_file)
        else:
            self.value_error('PKG_FILES', pkg_files)

    def parse_pkg_chksum(self, pkg_chksum):
        """
        Parse Package checksum
        - pkg_chksum: Python str/unicode
        """
        if isinstance(pkg_chksum, six.string_types):
            try:
                return self.builder.set_pkg_chk_sum(self.document, pkg_chksum)
            except CardinalityError:
                self.more_than_one_error('PKG_CHECKSUM')
            except OrderError:
                self.order_error('PKG_CHECKSUM', 'PKG_NAME')
        elif pkg_chksum is not None:
            self.value_error('PKG_CHECKSUM', pkg_chksum)


class Parser(CreationInfoParser, ExternalDocumentRefsParser, LicenseParser,
            AnnotationParser, SnippetParser, ReviewParser, FileParser, PackageParser):

    def __init__(self, builder, logger):
        super(Parser, self).__init__(builder, logger)

    def parse(self):
        """
        Parse Document Information fields
        """
        self.error = False
        self.document = document.Document()
        if not isinstance(self.document_object, dict):
            self.logger.log('Empty or not valid SPDX Document')
            self.error = True
            return self.document, self.error

        self.parse_doc_version(self.document_object.get('specVersion'))
        self.parse_doc_data_license(self.document_object.get('dataLicense'))
        self.parse_doc_id(self.document_object.get('id'))
        self.parse_doc_name(self.document_object.get('name'))
        self.parse_doc_namespace(self.document_object.get('namespace'))
        self.parse_doc_comment(self.document_object.get('comment'))
        self.parse_creation_info(self.document_object.get('creationInfo'))
        self.parse_external_document_refs(self.document_object.get('externalDocumentRefs'))
        self.parse_extracted_license_info(self.document_object.get('extractedLicenseInfos'))
        self.parse_annotations(self.document_object.get('annotations'))
        self.parse_reviews(self.document_object.get('reviewers'))
        self.parse_snippets(self.document_object.get('snippets'))

        self.parse_doc_described_objects(self.document_object.get('documentDescribes'))

        validation_messages = []
        # Report extra errors if self.error is False otherwise there will be
        # redundent messages
        validation_messages = self.document.validate(validation_messages)
        if not self.error:
            if validation_messages:
                for msg in validation_messages:
                    self.logger.log(msg)
                self.error = True

        return self.document, self.error

    def parse_doc_version(self, doc_version):
        """
        Parse Document version
        - doc_version: Python str/unicode
        """
        if isinstance(doc_version, six.string_types):
            try:
                return self.builder.set_doc_version(self.document, doc_version)
            except SPDXValueError:
                self.value_error('DOC_VERS_VALUE', doc_version)
            except CardinalityError:
                self.more_than_one_error('DOC_VERS_VALUE')
        else:
            self.value_error('DOC_VERS_VALUE', doc_version)

    def parse_doc_data_license(self, doc_data_license):
        """
        Parse Document data license
        - doc_data_license: Python str/unicode
        """
        try:
            return self.builder.set_doc_data_lics(self.document, doc_data_license)
        except SPDXValueError:
            self.value_error('DOC_D_LICS', doc_data_license)
        except CardinalityError:
            self.more_than_one_error('DOC_D_LICS')

    def parse_doc_id(self, doc_id):
        """
        Parse Document SPDX id
        - doc_id: Python str/unicode
        """
        if isinstance(doc_id, six.string_types):
            try:
                return self.builder.set_doc_spdx_id(self.document, doc_id)
            except SPDXValueError:
                self.value_error('DOC_SPDX_ID_VALUE', doc_id)
            except CardinalityError:
                self.more_than_one_error('DOC_SPDX_ID_VALUE')
        else:
            self.value_error('DOC_SPDX_ID_VALUE', doc_id)

    def parse_doc_name(self, doc_name):
        """
        Parse Document name
        - doc_name: Python str/unicode
        """
        if isinstance(doc_name, six.string_types):
            try:
                return self.builder.set_doc_name(self.document, doc_name)
            except CardinalityError:
                self.more_than_one_error('DOC_NAME_VALUE')
        else:
            self.value_error('DOC_NAME_VALUE', doc_name)

    def parse_doc_namespace(self, doc_namespace):
        """
        Parse Document namespace
        - doc_namespace: Python str/unicode
        """
        if isinstance(doc_namespace, six.string_types):
            try:
                return self.builder.set_doc_namespace(self.document, doc_namespace)
            except SPDXValueError:
                self.value_error('DOC_NAMESPACE_VALUE', doc_namespace)
            except CardinalityError:
                self.more_than_one_error('DOC_NAMESPACE_VALUE')
        else:
            self.value_error('DOC_NAMESPACE_VALUE', doc_namespace)

    def parse_doc_comment(self, doc_comment):
        """
        Parse Document comment
        - doc_comment: Python str/unicode
        """
        if isinstance(doc_comment, six.string_types):
            try:
                return self.builder.set_doc_comment(self.document, doc_comment)
            except CardinalityError:
                self.more_than_one_error('DOC_COMMENT_VALUE')
        elif doc_comment is not None:
            self.value_error('DOC_COMMENT_VALUE', doc_comment)

    def parse_doc_described_objects(self, doc_described_objects):
        """
        Parse Document documentDescribes (Files and Packages dicts)
        - doc_described_objects: Python list of dicts as in FileParser.parse_file or PackageParser.parse_package
        """
        if isinstance(doc_described_objects, list):
            packages = filter(lambda described: isinstance(described, dict) and described.get('Package') is not None, doc_described_objects)
            files = filter(lambda described: isinstance(described, dict) and described.get('File') is not None, doc_described_objects)
            # At the moment, only single-package documents are supported, so just the last package will be stored.
            for package in packages:
                self.parse_package(package.get('Package'))
            for file in files:
                self.parse_file(file.get('File'))
            return True
        else:
            self.value_error('DOC_DESCRIBES', doc_described_objects)
