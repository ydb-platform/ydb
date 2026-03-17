
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

import re

from six import string_types

from spdx import annotation
from spdx import checksum
from spdx import creationinfo
from spdx import document
from spdx import file
from spdx import package
from spdx import review
from spdx import snippet
from spdx import utils
from spdx import version

from spdx.document import ExternalDocumentRef
from spdx.parsers.builderexceptions import CardinalityError
from spdx.parsers.builderexceptions import OrderError
from spdx.parsers.builderexceptions import SPDXValueError
from spdx.parsers import validations


def checksum_from_sha1(value):
    """
    Return an spdx.checksum.Algorithm instance representing the SHA1
    checksum or None if does not match CHECKSUM_RE.
    """
    # More constrained regex at lexer level
    CHECKSUM_RE = re.compile('SHA1:\\s*([\\S]+)', re.UNICODE)
    match = CHECKSUM_RE.match(value)
    if match:
        return checksum.Algorithm(identifier='SHA1', value=match.group(1))
    else:
        return None


def str_from_text(text):
    """
    Return content of a free form text block as a string.
    """
    REGEX = re.compile('<text>((.|\n)+)</text>', re.UNICODE)
    match = REGEX.match(text)
    if match:
        return match.group(1)
    else:
        return None


class DocBuilder(object):
    """
    Set the fields of the top level document model.
    """
    VERS_STR_REGEX = re.compile(r'SPDX-(\d+)\.(\d+)', re.UNICODE)

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_document()

    def set_doc_version(self, doc, value):
        """
        Set the document version.
        Raise SPDXValueError if malformed value.
        Raise CardinalityError if already defined.
        """
        if not self.doc_version_set:
            self.doc_version_set = True
            m = self.VERS_STR_REGEX.match(value)
            if m is None:
                raise SPDXValueError('Document::Version')
            else:
                doc.version = version.Version(major=int(m.group(1)),
                                              minor=int(m.group(2)))
                return True
        else:
            raise CardinalityError('Document::Version')

    def set_doc_data_lics(self, doc, lics):
        """
        Set the document data license.
        Raise value error if malformed value
        Raise CardinalityError if already defined.
        """
        if not self.doc_data_lics_set:
            self.doc_data_lics_set = True
            if validations.validate_data_lics(lics):
                doc.data_license = document.License.from_identifier(lics)
                return True
            else:
                raise SPDXValueError('Document::DataLicense')
        else:
            raise CardinalityError('Document::DataLicense')

    def set_doc_name(self, doc, name):
        """
        Set the document name.
        Raise CardinalityError if already defined.
        """
        if not self.doc_name_set:
            doc.name = name
            self.doc_name_set = True
            return True
        else:
            raise CardinalityError('Document::Name')

    def set_doc_spdx_id(self, doc, doc_spdx_id_line):
        """
        Set the document SPDX Identifier.
        Raise value error if malformed value.
        Raise CardinalityError if already defined.
        """
        if not self.doc_spdx_id_set:
            if doc_spdx_id_line == 'SPDXRef-DOCUMENT':
                doc.spdx_id = doc_spdx_id_line
                self.doc_spdx_id_set = True
                return True
            else:
                raise SPDXValueError('Document::SPDXID')
        else:
            raise CardinalityError('Document::SPDXID')

    def set_doc_comment(self, doc, comment):
        """
        Set document comment.
        Raise CardinalityError if comment already set.
        Raise SPDXValueError if comment is not free form text.
        """
        if not self.doc_comment_set:
            self.doc_comment_set = True
            if validations.validate_doc_comment(comment):
                doc.comment = str_from_text(comment)
                return True
            else:
                raise SPDXValueError('Document::Comment')
        else:
            raise CardinalityError('Document::Comment')

    def set_doc_namespace(self, doc, namespace):
        """
        Set the document namespace.
        Raise SPDXValueError if malformed value.
        Raise CardinalityError if already defined.
        """
        if not self.doc_namespace_set:
            self.doc_namespace_set = True
            if validations.validate_doc_namespace(namespace):
                doc.namespace = namespace
                return True
            else:
                raise SPDXValueError('Document::Namespace')
        else:
            raise CardinalityError('Document::Comment')

    def reset_document(self):
        """
        Reset the state to allow building new documents
        """
        # FIXME: this state does not make sense
        self.doc_version_set = False
        self.doc_comment_set = False
        self.doc_namespace_set = False
        self.doc_data_lics_set = False
        self.doc_name_set = False
        self.doc_spdx_id_set = False


class ExternalDocumentRefBuilder(object):

    def set_ext_doc_id(self, doc, ext_doc_id):
        """
        Set the `external_document_id` attribute of the `ExternalDocumentRef` object.
        """
        doc.add_ext_document_reference(
            ExternalDocumentRef(
                external_document_id=ext_doc_id))

    def set_spdx_doc_uri(self, doc, spdx_doc_uri):
        """
        Set the `spdx_document_uri` attribute of the `ExternalDocumentRef` object.
        """
        if validations.validate_doc_namespace(spdx_doc_uri):
            doc.ext_document_references[-1].spdx_document_uri = spdx_doc_uri
        else:
            raise SPDXValueError('Document::ExternalDocumentRef')

    def set_chksum(self, doc, chksum):
        """
        Set the `check_sum` attribute of the `ExternalDocumentRef` object.
        """
        doc.ext_document_references[-1].check_sum = checksum_from_sha1(
            chksum)

    def add_ext_doc_refs(self, doc, ext_doc_id, spdx_doc_uri, chksum):
        self.set_ext_doc_id(doc, ext_doc_id)
        self.set_spdx_doc_uri(doc, spdx_doc_uri)
        self.set_chksum(doc, chksum)


class EntityBuilder(object):

    tool_re = re.compile(r'Tool:\s*(.+)', re.UNICODE)
    person_re = re.compile(r'Person:\s*(([^(])+)(\((.*)\))?', re.UNICODE)
    org_re = re.compile(r'Organization:\s*(([^(])+)(\((.*)\))?', re.UNICODE)
    PERSON_NAME_GROUP = 1
    PERSON_EMAIL_GROUP = 4
    ORG_NAME_GROUP = 1
    ORG_EMAIL_GROUP = 4
    TOOL_NAME_GROUP = 1

    def build_tool(self, doc, entity):
        """
        Build a tool object out of a string representation.
        Return built tool.
        Raise SPDXValueError if failed to extract tool name or name is malformed
        """
        match = self.tool_re.match(entity)
        if match and validations.validate_tool_name(match.group(self.TOOL_NAME_GROUP)):
            name = match.group(self.TOOL_NAME_GROUP)
            return creationinfo.Tool(name)
        else:
            raise SPDXValueError('Failed to extract tool name')

    def build_org(self, doc, entity):
        """
        Build an organization object of of a string representation.
        Return built organization.
        Raise SPDXValueError if failed to extractname.
        """
        match = self.org_re.match(entity)
        if match and validations.validate_org_name(match.group(self.ORG_NAME_GROUP)):
            name = match.group(self.ORG_NAME_GROUP).strip()
            email = match.group(self.ORG_EMAIL_GROUP)
            if (email is not None) and (len(email) != 0):
                return creationinfo.Organization(name=name, email=email.strip())
            else:
                return creationinfo.Organization(name=name, email=None)
        else:
            raise SPDXValueError('Failed to extract Organization name')

    def build_person(self, doc, entity):
        """
        Build an organization object of of a string representation.
        Return built organization. Raise SPDXValueError if failed to extract name.
        """
        match = self.person_re.match(entity)
        if match and validations.validate_person_name(match.group(self.PERSON_NAME_GROUP)):
            name = match.group(self.PERSON_NAME_GROUP).strip()
            email = match.group(self.PERSON_EMAIL_GROUP)
            if (email is not None) and (len(email) != 0):
                return creationinfo.Person(name=name, email=email.strip())
            else:
                return creationinfo.Person(name=name, email=None)
        else:
            raise SPDXValueError('Failed to extract person name')


class CreationInfoBuilder(object):

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_creation_info()

    def add_creator(self, doc, creator):
        """
        Add a creator to the document's creation info.
        Return true if creator is valid.
        Creator must be built by an EntityBuilder.
        Raise SPDXValueError if not a creator type.
        """
        if validations.validate_creator(creator):
            doc.creation_info.add_creator(creator)
            return True
        else:
            raise SPDXValueError('CreationInfo::Creator')

    def set_created_date(self, doc, created):
        """
        Set created date.
        Raise CardinalityError if created date already set.
        Raise SPDXValueError if created is not a date.
        """
        if not self.created_date_set:
            self.created_date_set = True
            date = utils.datetime_from_iso_format(created)
            if date is not None:
                doc.creation_info.created = date
                return True
            else:
                raise SPDXValueError('CreationInfo::Date')
        else:
            raise CardinalityError('CreationInfo::Created')

    def set_creation_comment(self, doc, comment):
        """
        Set creation comment.
        Raise CardinalityError if comment already set.
        Raise SPDXValueError if not free form text.
        """
        if not self.creation_comment_set:
            self.creation_comment_set = True
            if validations.validate_creation_comment(comment):
                doc.creation_info.comment = str_from_text(comment)
                return True
            else:
                raise SPDXValueError('CreationInfo::Comment')
        else:
            raise CardinalityError('CreationInfo::Comment')

    def set_lics_list_ver(self, doc, value):
        """
        Set the license list version.
        Raise CardinalityError if already set.
        Raise SPDXValueError if incorrect value.
        """
        if not self.lics_list_ver_set:
            self.lics_list_ver_set = True
            vers = version.Version.from_str(value)
            if vers is not None:
                doc.creation_info.license_list_version = vers
                return True
            else:
                raise SPDXValueError('CreationInfo::LicenseListVersion')
        else:
            raise CardinalityError('CreationInfo::LicenseListVersion')

    def reset_creation_info(self):
        """
        Reset builder state to allow building new creation info.
        """
        # FIXME: this state does not make sense
        self.created_date_set = False
        self.creation_comment_set = False
        self.lics_list_ver_set = False


class ReviewBuilder(object):

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_reviews()

    def reset_reviews(self):
        """
        Reset the builder's state to allow building new reviews.
        """
        # FIXME: this state does not make sense
        self.review_date_set = False
        self.review_comment_set = False

    def add_reviewer(self, doc, reviewer):
        """
        Adds a reviewer to the SPDX Document.
        Reviwer is an entity created by an EntityBuilder.
        Raise SPDXValueError if not a valid reviewer type.
        """
        # Each reviewer marks the start of a new review object.
        # FIXME: this state does not make sense
        self.reset_reviews()
        if validations.validate_reviewer(reviewer):
            doc.add_review(review.Review(reviewer=reviewer))
            return True
        else:
            raise SPDXValueError('Review::Reviewer')

    def add_review_date(self, doc, reviewed):
        """
        Set the review date.
        Raise CardinalityError if already set.
        Raise OrderError if no reviewer defined before.
        Raise SPDXValueError if invalid reviewed value.
        """
        if len(doc.reviews) != 0:
            if not self.review_date_set:
                self.review_date_set = True
                date = utils.datetime_from_iso_format(reviewed)
                if date is not None:
                    doc.reviews[-1].review_date = date
                    return True
                else:
                    raise SPDXValueError('Review::ReviewDate')
            else:
                raise CardinalityError('Review::ReviewDate')
        else:
            raise OrderError('Review::ReviewDate')

    def add_review_comment(self, doc, comment):
        """
        Set the review comment.
        Raise CardinalityError if already set.
        Raise OrderError if no reviewer defined before.
        Raise SPDXValueError if comment is not free form text.
        """
        if len(doc.reviews) != 0:
            if not self.review_comment_set:
                self.review_comment_set = True
                if validations.validate_review_comment(comment):
                    doc.reviews[-1].comment = str_from_text(comment)
                    return True
                else:
                    raise SPDXValueError('ReviewComment::Comment')
            else:
                raise CardinalityError('ReviewComment')
        else:
            raise OrderError('ReviewComment')


class AnnotationBuilder(object):

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_annotations()

    def reset_annotations(self):
        """
        Reset the builder's state to allow building new annotations.
        """
        # FIXME: this state does not make sense
        self.annotation_date_set = False
        self.annotation_comment_set = False
        self.annotation_type_set = False
        self.annotation_spdx_id_set = False

    def add_annotator(self, doc, annotator):
        """
        Add an annotator to the SPDX Document.
        Annotator is an entity created by an EntityBuilder.
        Raise SPDXValueError if not a valid annotator type.
        """
        # Each annotator marks the start of a new annotation object.
        # FIXME: this state does not make sense
        self.reset_annotations()
        if validations.validate_annotator(annotator):
            doc.add_annotation(annotation.Annotation(annotator=annotator))
            return True
        else:
            raise SPDXValueError('Annotation::Annotator')

    def add_annotation_date(self, doc, annotation_date):
        """
        Set the annotation date.
        Raise CardinalityError if already set.
        Raise OrderError if no annotator defined before.
        Raise SPDXValueError if invalid value.
        """
        if len(doc.annotations) != 0:
            if not self.annotation_date_set:
                self.annotation_date_set = True
                date = utils.datetime_from_iso_format(annotation_date)
                if date is not None:
                    doc.annotations[-1].annotation_date = date
                    return True
                else:
                    raise SPDXValueError('Annotation::AnnotationDate')
            else:
                raise CardinalityError('Annotation::AnnotationDate')
        else:
            raise OrderError('Annotation::AnnotationDate')

    def add_annotation_comment(self, doc, comment):
        """
        Set the annotation comment.
        Raise CardinalityError if already set.
        Raise OrderError if no annotator defined before.
        Raise SPDXValueError if comment is not free form text.
        """
        if len(doc.annotations) != 0:
            if not self.annotation_comment_set:
                self.annotation_comment_set = True
                if validations.validate_annotation_comment(comment):
                    doc.annotations[-1].comment = str_from_text(comment)
                    return True
                else:
                    raise SPDXValueError('AnnotationComment::Comment')
            else:
                raise CardinalityError('AnnotationComment::Comment')
        else:
            raise OrderError('AnnotationComment::Comment')

    def add_annotation_type(self, doc, annotation_type):
        """
        Set the annotation type.
        Raise CardinalityError if already set.
        Raise OrderError if no annotator defined before.
        Raise SPDXValueError if invalid value.
        """
        if len(doc.annotations) != 0:
            if not self.annotation_type_set:
                self.annotation_type_set = True
                if validations.validate_annotation_type(annotation_type):
                    doc.annotations[-1].annotation_type = annotation_type
                    return True
                else:
                    raise SPDXValueError('Annotation::AnnotationType')
            else:
                raise CardinalityError('Annotation::AnnotationType')
        else:
            raise OrderError('Annotation::AnnotationType')

    def set_annotation_spdx_id(self, doc, spdx_id):
        """
        Set the annotation SPDX Identifier.
        Raise CardinalityError if already set.
        Raise OrderError if no annotator defined before.
        """
        if len(doc.annotations) != 0:
            if not self.annotation_spdx_id_set:
                self.annotation_spdx_id_set = True
                doc.annotations[-1].spdx_id = spdx_id
                return True
            else:
                raise CardinalityError('Annotation::SPDXREF')
        else:
            raise OrderError('Annotation::SPDXREF')


class PackageBuilder(object):
    VERIF_CODE_REGEX = re.compile(r"([0-9a-f]+)\s*(\(\s*(.+)\))?", re.UNICODE)
    VERIF_CODE_CODE_GRP = 1
    VERIF_CODE_EXC_FILES_GRP = 3

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_package()

    def reset_package(self):
        """Resets the builder's state in order to build new packages."""
        # FIXME: this state does not make sense
        self.package_set = False
        self.package_spdx_id_set = False
        self.package_vers_set = False
        self.package_file_name_set = False
        self.package_supplier_set = False
        self.package_originator_set = False
        self.package_down_location_set = False
        self.package_files_analyzed_set = False
        self.package_home_set = False
        self.package_verif_set = False
        self.package_chk_sum_set = False
        self.package_source_info_set = False
        self.package_conc_lics_set = False
        self.package_license_declared_set = False
        self.package_license_comment_set = False
        self.package_cr_text_set = False
        self.package_summary_set = False
        self.package_desc_set = False
        self.package_comment_set = False
        self.pkg_ext_comment_set = False

    def create_package(self, doc, name):
        """
        Create a package for the SPDX Document.
        name - any string.
        Raise CardinalityError if package already defined.
        """
        if not self.package_set:
            self.package_set = True
            doc.package = package.Package(name=name)
            return True
        else:
            raise CardinalityError('Package::Name')

    def set_pkg_spdx_id(self, doc, spdx_id):
        """
        Set the Package SPDX Identifier.
        Raise SPDXValueError if malformed value.
        Raise CardinalityError if already defined.
        """
        self.assert_package_exists()
        if not self.package_spdx_id_set:
            if validations.validate_pkg_spdx_id(spdx_id):
                doc.package.spdx_id = spdx_id
                self.package_spdx_id_set = True
                return True
            else:
                raise SPDXValueError('Package::SPDXID')
        else:
            raise CardinalityError('Package::SPDXID')

    def set_pkg_vers(self, doc, version):
        """
        Set package version, if not already set.
        version - Any string.
        Raise CardinalityError if already has a version.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_vers_set:
            self.package_vers_set = True
            doc.package.version = version
            return True
        else:
            raise CardinalityError('Package::Version')

    def set_pkg_file_name(self, doc, name):
        """
        Set the package file name, if not already set.
        name - Any string.
        Raise CardinalityError if already has a file_name.
        Raise OrderError if no pacakge previously defined.
        """
        self.assert_package_exists()
        if not self.package_file_name_set:
            self.package_file_name_set = True
            doc.package.file_name = name
            return True
        else:
            raise CardinalityError('Package::FileName')

    def set_pkg_supplier(self, doc, entity):
        """
        Set the package supplier, if not already set.
        entity - Organization, Person or NoAssert.
        Raise CardinalityError if already has a supplier.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_supplier_set:
            self.package_supplier_set = True
            if validations.validate_pkg_supplier(entity):
                doc.package.supplier = entity
                return True
            else:
                raise SPDXValueError('Package::Supplier')
        else:
            raise CardinalityError('Package::Supplier')

    def set_pkg_originator(self, doc, entity):
        """
        Set the package originator, if not already set.
        entity - Organization, Person or NoAssert.
        Raise CardinalityError if already has an originator.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_originator_set:
            self.package_originator_set = True
            if validations.validate_pkg_originator(entity):
                doc.package.originator = entity
                return True
            else:
                raise SPDXValueError('Package::Originator')
        else:
            raise CardinalityError('Package::Originator')

    def set_pkg_down_location(self, doc, location):
        """
        Set the package download location, if not already set.
        location - A string
        Raise CardinalityError if already defined.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_down_location_set:
            self.package_down_location_set = True
            doc.package.download_location = location
            return True
        else:
            raise CardinalityError('Package::DownloadLocation')

    def set_pkg_files_analyzed(self, doc, files_analyzed):
        """
        Set the package files analyzed, if not already set.
        Raise SPDXValueError if malformed value, CardinalityError if
        already defined.
        """
        self.assert_package_exists()
        if not self.package_files_analyzed_set:
            if files_analyzed:
                if validations.validate_pkg_files_analyzed(files_analyzed):
                    self.package_files_analyzed_set = True
                    doc.package.files_analyzed = files_analyzed
                    print(doc.package.files_analyzed)
                    return True
                else:
                    raise SPDXValueError('Package::FilesAnalyzed')
        else:
            raise CardinalityError('Package::FilesAnalyzed')

    def set_pkg_home(self, doc, location):
        """Set the package homepage location if not already set.
        location - A string or None or NoAssert.
        Raise CardinalityError if already defined.
        Raise OrderError if no package previously defined.
        Raise SPDXValueError if location has incorrect value.
        """
        self.assert_package_exists()
        if not self.package_home_set:
            self.package_home_set = True
            if validations.validate_pkg_homepage(location):
                doc.package.homepage = location
                return True
            else:
                raise SPDXValueError('Package::HomePage')
        else:
            raise CardinalityError('Package::HomePage')

    def set_pkg_verif_code(self, doc, code):
        """
        Set the package verification code, if not already set.
        code - A string.
        Raise CardinalityError if already defined.
        Raise OrderError if no package previously defined.
        Raise Value error if doesn't match verifcode form
        """
        self.assert_package_exists()
        if not self.package_verif_set:
            self.package_verif_set = True
            match = self.VERIF_CODE_REGEX.match(code)
            if match:
                doc.package.verif_code = match.group(self.VERIF_CODE_CODE_GRP)
                if match.group(self.VERIF_CODE_EXC_FILES_GRP) is not None:
                    doc.package.verif_exc_files = match.group(self.VERIF_CODE_EXC_FILES_GRP).split(',')
                return True
            else:
                raise SPDXValueError('Package::VerificationCode')
        else:
            raise CardinalityError('Package::VerificationCode')

    def set_pkg_chk_sum(self, doc, chk_sum):
        """
        Set the package check sum, if not already set.
        chk_sum - A string
        Raise CardinalityError if already defined.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_chk_sum_set:
            self.package_chk_sum_set = True
            doc.package.check_sum = checksum_from_sha1(chk_sum)
            return True
        else:
            raise CardinalityError('Package::CheckSum')

    def set_pkg_source_info(self, doc, text):
        """
        Set the package's source information, if not already set.
        text - Free form text.
        Raise CardinalityError if already defined.
        Raise OrderError if no package previously defined.
        SPDXValueError if text is not free form text.
        """
        self.assert_package_exists()
        if not self.package_source_info_set:
            self.package_source_info_set = True
            if validations.validate_pkg_src_info(text):
                doc.package.source_info = str_from_text(text)
                return True
            else:
                raise SPDXValueError('Pacckage::SourceInfo')
        else:
            raise CardinalityError('Package::SourceInfo')

    def set_pkg_licenses_concluded(self, doc, licenses):
        """
        Set the package's concluded licenses.
        licenses - License info.
        Raise CardinalityError if already defined.
        Raise OrderError if no package previously defined.
        Raise SPDXValueError if data malformed.
        """
        self.assert_package_exists()
        if not self.package_conc_lics_set:
            self.package_conc_lics_set = True
            if validations.validate_lics_conc(licenses):
                doc.package.conc_lics = licenses
                return True
            else:
                raise SPDXValueError('Package::ConcludedLicenses')
        else:
            raise CardinalityError('Package::ConcludedLicenses')

    def set_pkg_license_from_file(self, doc, lic):
        """
        Add a license from a file to the package.
        Raise SPDXValueError if data malformed.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if validations.validate_lics_from_file(lic):
            doc.package.licenses_from_files.append(lic)
            return True
        else:
            raise SPDXValueError('Package::LicensesFromFile')

    def set_pkg_license_declared(self, doc, lic):
        """
        Set the package's declared license.
        Raise SPDXValueError if data malformed.
        Raise OrderError if no package previously defined.
        Raise CardinalityError if already set.
        """
        self.assert_package_exists()
        if not self.package_license_declared_set:
            self.package_license_declared_set = True
            if validations.validate_lics_conc(lic):
                doc.package.license_declared = lic
                return True
            else:
                raise SPDXValueError('Package::LicenseDeclared')
        else:
            raise CardinalityError('Package::LicenseDeclared')

    def set_pkg_license_comment(self, doc, text):
        """
        Set the package's license comment.
        Raise OrderError if no package previously defined.
        Raise CardinalityError if already set.
        Raise SPDXValueError if text is not free form text.
        """
        self.assert_package_exists()
        if not self.package_license_comment_set:
            self.package_license_comment_set = True
            if validations.validate_pkg_lics_comment(text):
                doc.package.license_comment = str_from_text(text)
                return True
            else:
                raise SPDXValueError('Package::LicenseComment')
        else:
            raise CardinalityError('Package::LicenseComment')

    def set_pkg_cr_text(self, doc, text):
        """
        Set the package's copyright text.
        Raise OrderError if no package previously defined.
        Raise CardinalityError if already set.
        Raise value error if text is not one of [None, NOASSERT, TEXT].
        """
        self.assert_package_exists()
        if not self.package_cr_text_set:
            self.package_cr_text_set = True
            if validations.validate_pkg_cr_text(text):
                if isinstance(text, string_types):
                    doc.package.cr_text = str_from_text(text)
                else:
                    doc.package.cr_text = text  # None or NoAssert
            else:
                raise SPDXValueError('Package::CopyrightText')
        else:
            raise CardinalityError('Package::CopyrightText')

    def set_pkg_summary(self, doc, text):
        """
        Set the package summary.
        Raise SPDXValueError if text is not free form text.
        Raise CardinalityError if summary already set.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_summary_set:
            self.package_summary_set = True
            if validations.validate_pkg_summary(text):
                doc.package.summary = str_from_text(text)
            else:
                raise SPDXValueError('Package::Summary')
        else:
            raise CardinalityError('Package::Summary')

    def set_pkg_desc(self, doc, text):
        """
        Set the package's description.
        Raise SPDXValueError if text is not free form text.
        Raise CardinalityError if description already set.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_desc_set:
            self.package_desc_set = True
            if validations.validate_pkg_desc(text):
                doc.package.description = str_from_text(text)
            else:
                raise SPDXValueError('Package::Description')
        else:
            raise CardinalityError('Package::Description')

    def set_pkg_comment(self, doc, text):
        """
        Set the package's comment.
        Raise SPDXValueError if text is not free form text.
        Raise CardinalityError if comment already set.
        Raise OrderError if no package previously defined.
        """
        self.assert_package_exists()
        if not self.package_comment_set:
            self.package_comment_set = True
            if validations.validate_pkg_comment(text):
                doc.package.comment = str_from_text(text)
            else:
                raise SPDXValueError('Package::Comment')
        else:
            raise CardinalityError('Package::Comment')

    def set_pkg_ext_ref_category(self, doc, category):
        """
        Set the `category` attribute of the `ExternalPackageRef` object.
        """
        self.assert_package_exists()
        if validations.validate_pkg_ext_ref_category(category):
            if (len(doc.package.pkg_ext_refs) and
                    doc.package.pkg_ext_refs[-1].category is None):
                doc.package.pkg_ext_refs[-1].category = category
            else:
                doc.package.add_pkg_ext_refs(
                    package.ExternalPackageRef(category=category))
        else:
            raise SPDXValueError('ExternalRef::Category')

    def set_pkg_ext_ref_type(self, doc, pkg_ext_ref_type):
        """
        Set the `pkg_ext_ref_type` attribute of the `ExternalPackageRef` object.
        """
        self.assert_package_exists()
        if validations.validate_pkg_ext_ref_type(pkg_ext_ref_type):
            if (len(doc.package.pkg_ext_refs) and
                    doc.package.pkg_ext_refs[-1].pkg_ext_ref_type is None):
                doc.package.pkg_ext_refs[-1].pkg_ext_ref_type = pkg_ext_ref_type
            else:
                doc.package.add_pkg_ext_refs(package.ExternalPackageRef(
                    pkg_ext_ref_type=pkg_ext_ref_type))
        else:
            raise SPDXValueError('ExternalRef::Type')

    def set_pkg_ext_ref_locator(self, doc, locator):
        """
        Set the `locator` attribute of the `ExternalPackageRef` object.
        """
        self.assert_package_exists()
        if (len(doc.package.pkg_ext_refs) and
                doc.package.pkg_ext_refs[-1].locator is None):
            doc.package.pkg_ext_refs[-1].locator = locator
        else:
            doc.package.add_pkg_ext_refs(package.ExternalPackageRef(
                locator=locator))

    def add_pkg_ext_ref_comment(self, doc, comment):
        """
        Set the `comment` attribute of the `ExternalPackageRef` object.
        """
        self.assert_package_exists()
        if not len(doc.package.pkg_ext_refs):
            raise OrderError('Package::ExternalRef')
        else:
            if validations.validate_pkg_ext_ref_comment(comment):
                doc.package.pkg_ext_refs[-1].comment = str_from_text(comment)
            else:
                raise SPDXValueError('ExternalRef::Comment')

    def add_pkg_ext_refs(self, doc, category, pkg_ext_ref_type, locator):
        self.set_pkg_ext_ref_category(doc, category)
        self.set_pkg_ext_ref_type(doc, pkg_ext_ref_type)
        self.set_pkg_ext_ref_locator(doc, locator)

    def assert_package_exists(self):
        if not self.package_set:
            raise OrderError('Package')


class FileBuilder(object):

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_file_stat()

    def set_file_name(self, doc, name):
        """
        Raise OrderError if no package defined.
        """
        if self.has_package(doc):
            doc.package.files.append(file.File(name))
            # A file name marks the start of a new file instance.
            # The builder must be reset
            # FIXME: this state does not make sense
            self.reset_file_stat()
            return True
        else:
            raise OrderError('File::Name')

    def set_file_spdx_id(self, doc, spdx_id):
        """
        Set the file SPDX Identifier.
        Raise OrderError if no package or no file defined.
        Raise SPDXValueError if malformed value.
        Raise CardinalityError if more than one spdx_id set.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_spdx_id_set:
                self.file_spdx_id_set = True
                if validations.validate_file_spdx_id(spdx_id):
                    self.file(doc).spdx_id = spdx_id
                    return True
                else:
                    raise SPDXValueError('File::SPDXID')
            else:
                raise CardinalityError('File::SPDXID')
        else:
            raise OrderError('File::SPDXID')

    def set_file_comment(self, doc, text):
        """
        Raise OrderError if no package or no file defined.
        Raise CardinalityError if more than one comment set.
        Raise SPDXValueError if text is not free form text.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_comment_set:
                self.file_comment_set = True
                if validations.validate_file_comment(text):
                    self.file(doc).comment = str_from_text(text)
                    return True
                else:
                    raise SPDXValueError('File::Comment')
            else:
                raise CardinalityError('File::Comment')
        else:
            raise OrderError('File::Comment')

    def set_file_type(self, doc, type_value):
        """
        Raise OrderError if no package or file defined.
        Raise CardinalityError if more than one type set.
        Raise SPDXValueError if type is unknown.
        """
        type_dict = {
            'SOURCE': file.FileType.SOURCE,
            'BINARY': file.FileType.BINARY,
            'ARCHIVE': file.FileType.ARCHIVE,
            'OTHER': file.FileType.OTHER
        }
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_type_set:
                self.file_type_set = True
                if type_value in type_dict.keys():
                    self.file(doc).type = type_dict[type_value]
                    return True
                else:
                    raise SPDXValueError('File::Type')
            else:
                raise CardinalityError('File::Type')
        else:
            raise OrderError('File::Type')

    def set_file_chksum(self, doc, chksum):
        """
        Raise OrderError if no package or file defined.
        Raise CardinalityError if more than one chksum set.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_chksum_set:
                self.file_chksum_set = True
                self.file(doc).chk_sum = checksum_from_sha1(chksum)
                return True
            else:
                raise CardinalityError('File::CheckSum')
        else:
            raise OrderError('File::CheckSum')

    def set_concluded_license(self, doc, lic):
        """
        Raise OrderError if no package or file defined.
        Raise CardinalityError if already set.
        Raise SPDXValueError if malformed.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_conc_lics_set:
                self.file_conc_lics_set = True
                if validations.validate_lics_conc(lic):
                    self.file(doc).conc_lics = lic
                    return True
                else:
                    raise SPDXValueError('File::ConcludedLicense')
            else:
                raise CardinalityError('File::ConcludedLicense')
        else:
            raise OrderError('File::ConcludedLicense')

    def set_file_license_in_file(self, doc, lic):
        """
        Raise OrderError if no package or file defined.
        Raise SPDXValueError if malformed value.
        """
        if self.has_package(doc) and self.has_file(doc):
            if validations.validate_file_lics_in_file(lic):
                self.file(doc).add_lics(lic)
                return True
            else:
                raise SPDXValueError('File::LicenseInFile')
        else:
            raise OrderError('File::LicenseInFile')

    def set_file_license_comment(self, doc, text):
        """
        Raise OrderError if no package or file defined.
        Raise SPDXValueError if text is not free form text.
        Raise CardinalityError if more than one per file.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_license_comment_set:
                self.file_license_comment_set = True
                if validations.validate_file_lics_comment(text):
                    self.file(doc).license_comment = str_from_text(text)
                else:
                    raise SPDXValueError('File::LicenseComment')
            else:
                raise CardinalityError('File::LicenseComment')
        else:
            raise OrderError('File::LicenseComment')

    def set_file_copyright(self, doc, text):
        """
        Raise OrderError if no package or file defined.
        Raise SPDXValueError if not free form text or NONE or NO_ASSERT.
        Raise CardinalityError if more than one.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_copytext_set:
                self.file_copytext_set = True
                if validations.validate_file_cpyright(text):
                    if isinstance(text, string_types):
                        self.file(doc).copyright = str_from_text(text)
                    else:
                        self.file(doc).copyright = text  # None or NoAssert
                    return True
                else:
                    raise SPDXValueError('File::CopyRight')
            else:
                raise CardinalityError('File::CopyRight')
        else:
            raise OrderError('File::CopyRight')

    def set_file_notice(self, doc, text):
        """
        Raise OrderError if no package or file defined.
        Raise SPDXValueError if not free form text.
        Raise CardinalityError if more than one.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_notice_set:
                self.file_notice_set = True
                if validations.validate_file_notice(text):
                    self.file(doc).notice = str_from_text(text)
                else:
                    raise SPDXValueError('File::Notice')
            else:
                raise CardinalityError('File::Notice')
        else:
            raise OrderError('File::Notice')

    def add_file_contribution(self, doc, value):
        """
        Raise OrderError if no package or file defined.
        """
        if self.has_package(doc) and self.has_file(doc):
            self.file(doc).add_contrib(value)
        else:
            raise OrderError('File::Contributor')

    def add_file_dep(self, doc, value):
        """
        Raise OrderError if no package or file defined.
        """
        if self.has_package(doc) and self.has_file(doc):
            self.file(doc).add_depend(value)
        else:
            raise OrderError('File::Dependency')

    def set_file_atrificat_of_project(self, doc, symbol, value):
        """
        Set a file name, uri or home artificat.
        Raise OrderError if no package or file defined.
        """
        if self.has_package(doc) and self.has_file(doc):
            self.file(doc).add_artifact(symbol, value)
        else:
            raise OrderError('File::Artificat')


    def file(self, doc):
        """
        Return the last file in the document's package's file list.
        """
        return doc.package.files[-1]

    def has_file(self, doc):
        """
        Return true if the document's package has at least one file.
        Does not test if the document has a package.
        """
        return len(doc.package.files) != 0

    def has_package(self, doc):
        """
        Return true if the document has a package.
        """
        return doc.package is not None

    def reset_file_stat(self):
        """
        Reset the builder's state to enable building new files.
        """
        # FIXME: this state does not make sense
        self.file_spdx_id_set = False
        self.file_comment_set = False
        self.file_type_set = False
        self.file_chksum_set = False
        self.file_conc_lics_set = False
        self.file_license_comment_set = False
        self.file_notice_set = False
        self.file_copytext_set = False


class LicenseBuilder(object):

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_extr_lics()

    def extr_lic(self, doc):
        """
        Retrieve last license in extracted license list.
        """
        return doc.extracted_licenses[-1]

    def has_extr_lic(self, doc):
        return len(doc.extracted_licenses) != 0

    def set_lic_id(self, doc, lic_id):
        """
        Add a new extracted license to the document.
        Raise SPDXValueError if data format is incorrect.
        """
        # FIXME: this state does not make sense
        self.reset_extr_lics()
        if validations.validate_extracted_lic_id(lic_id):
            doc.add_extr_lic(document.ExtractedLicense(lic_id))
            return True
        else:
            raise SPDXValueError('ExtractedLicense::id')

    def set_lic_text(self, doc, text):
        """
        Set license extracted text.
        Raise SPDXValueError if text is not free form text.
        Raise OrderError if no license ID defined.
        """
        if self.has_extr_lic(doc):
            if not self.extr_text_set:
                self.extr_text_set = True
                if validations.validate_is_free_form_text(text):
                    self.extr_lic(doc).text = str_from_text(text)
                    return True
                else:
                    raise SPDXValueError('ExtractedLicense::text')
            else:
                raise CardinalityError('ExtractedLicense::text')
        else:
            raise OrderError('ExtractedLicense::text')

    def set_lic_name(self, doc, name):
        """
        Set license name.
        Raise SPDXValueError if name is not str or utils.NoAssert
        Raise OrderError if no license id defined.
        """
        if self.has_extr_lic(doc):
            if not self.extr_lic_name_set:
                self.extr_lic_name_set = True
                if validations.validate_extr_lic_name(name):
                    self.extr_lic(doc).full_name = name
                    return True
                else:
                    raise SPDXValueError('ExtractedLicense::Name')
            else:
                raise CardinalityError('ExtractedLicense::Name')
        else:
            raise OrderError('ExtractedLicense::Name')

    def set_lic_comment(self, doc, comment):
        """
        Set license comment.
        Raise SPDXValueError if comment is not free form text.
        Raise OrderError if no license ID defined.
        """
        if self.has_extr_lic(doc):
            if not self.extr_lic_comment_set:
                self.extr_lic_comment_set = True
                if validations.validate_is_free_form_text(comment):
                    self.extr_lic(doc).comment = str_from_text(comment)
                    return True
                else:
                    raise SPDXValueError('ExtractedLicense::comment')
            else:
                raise CardinalityError('ExtractedLicense::comment')
        else:
            raise OrderError('ExtractedLicense::comment')

    def add_lic_xref(self, doc, ref):
        """
        Add a license cross reference.
        Raise OrderError if no License ID defined.
        """
        if self.has_extr_lic(doc):
            self.extr_lic(doc).add_xref(ref)
            return True
        else:
            raise OrderError('ExtractedLicense::CrossRef')

    def reset_extr_lics(self):
        # FIXME: this state does not make sense
        self.extr_text_set = False
        self.extr_lic_name_set = False
        self.extr_lic_comment_set = False


class SnippetBuilder(object):

    def __init__(self):
        # FIXME: this state does not make sense
        self.reset_snippet()

    def create_snippet(self, doc, spdx_id):
        """
        Create a snippet for the SPDX Document.
        spdx_id - To uniquely identify any element in an SPDX document which
        may be referenced by other elements.
        Raise SPDXValueError if the data is a malformed value.
        """
        self.reset_snippet()
        spdx_id = spdx_id.split('#')[-1]
        if validations.validate_snippet_spdx_id(spdx_id):
            doc.add_snippet(snippet.Snippet(spdx_id=spdx_id))
            self.snippet_spdx_id_set = True
            return True
        else:
            raise SPDXValueError('Snippet::SnippetSPDXID')

    def set_snippet_name(self, doc, name):
        """
        Set name of the snippet.
        Raise OrderError if no snippet previously defined.
        Raise CardinalityError if the name is already set.
        """
        self.assert_snippet_exists()
        if not self.snippet_name_set:
            self.snippet_name_set = True
            doc.snippet[-1].name = name
            return True
        else:
            raise CardinalityError('SnippetName')

    def set_snippet_comment(self, doc, comment):
        """
        Set general comments about the snippet.
        Raise OrderError if no snippet previously defined.
        Raise SPDXValueError if the data is a malformed value.
        Raise CardinalityError if comment already set.
        """
        self.assert_snippet_exists()
        if not self.snippet_comment_set:
            self.snippet_comment_set = True
            if validations.validate_snip_comment(comment):
                doc.snippet[-1].comment = str_from_text(comment)
                return True
            else:
                raise SPDXValueError('Snippet::SnippetComment')
        else:
            raise CardinalityError('Snippet::SnippetComment')

    def set_snippet_copyright(self, doc, text):
        """Set the snippet's copyright text.
        Raise OrderError if no snippet previously defined.
        Raise CardinalityError if already set.
        Raise SPDXValueError if text is not one of [None, NOASSERT, TEXT].
        """
        self.assert_snippet_exists()
        if not self.snippet_copyright_set:
            self.snippet_copyright_set = True
            if validations.validate_snippet_copyright(text):
                if isinstance(text, string_types):
                    doc.snippet[-1].copyright = str_from_text(text)
                else:
                    doc.snippet[-1].copyright = text  # None or NoAssert
            else:
                raise SPDXValueError('Snippet::SnippetCopyrightText')
        else:
            raise CardinalityError('Snippet::SnippetCopyrightText')

    def set_snippet_lic_comment(self, doc, text):
        """
        Set the snippet's license comment.
        Raise OrderError if no snippet previously defined.
        Raise CardinalityError if already set.
        Raise SPDXValueError if the data is a malformed value.
        """
        self.assert_snippet_exists()
        if not self.snippet_lic_comment_set:
            self.snippet_lic_comment_set = True
            if validations.validate_snip_lic_comment(text):
                doc.snippet[-1].license_comment = str_from_text(text)
                return True
            else:
                raise SPDXValueError('Snippet::SnippetLicenseComments')
        else:
            raise CardinalityError('Snippet::SnippetLicenseComments')

    def set_snip_from_file_spdxid(self, doc, snip_from_file_spdxid):
        """
        Set the snippet's 'Snippet from File SPDX Identifier'.
        Raise OrderError if no snippet previously defined.
        Raise CardinalityError if already set.
        Raise SPDXValueError if the data is a malformed value.
        """
        self.assert_snippet_exists()
        snip_from_file_spdxid = snip_from_file_spdxid.split('#')[-1]
        if not self.snip_file_spdxid_set:
            self.snip_file_spdxid_set = True
            if validations.validate_snip_file_spdxid(snip_from_file_spdxid):
                doc.snippet[-1].snip_from_file_spdxid = snip_from_file_spdxid
                return True
            else:
                raise SPDXValueError('Snippet::SnippetFromFileSPDXID')
        else:
            raise CardinalityError('Snippet::SnippetFromFileSPDXID')

    def set_snip_concluded_license(self, doc, conc_lics):
        """
        Raise OrderError if no snippet previously defined.
        Raise CardinalityError if already set.
        Raise SPDXValueError if the data is a malformed value.
        """
        self.assert_snippet_exists()
        if not self.snippet_conc_lics_set:
            self.snippet_conc_lics_set = True
            if validations.validate_lics_conc(conc_lics):
                doc.snippet[-1].conc_lics = conc_lics
                return True
            else:
                raise SPDXValueError('Snippet::SnippetLicenseConcluded')
        else:
            raise CardinalityError('Snippet::SnippetLicenseConcluded')

    def set_snippet_lics_info(self, doc, lics_info):
        """
        Raise OrderError if no snippet previously defined.
        Raise SPDXValueError if the data is a malformed value.
        """
        self.assert_snippet_exists()
        if validations.validate_snip_lics_info(lics_info):
            doc.snippet[-1].add_lics(lics_info)
            return True
        else:
            raise SPDXValueError('Snippet::LicenseInfoInSnippet')

    def reset_snippet(self):
        # FIXME: this state does not make sense
        self.snippet_spdx_id_set = False
        self.snippet_name_set = False
        self.snippet_comment_set = False
        self.snippet_copyright_set = False
        self.snippet_lic_comment_set = False
        self.snip_file_spdxid_set = False
        self.snippet_conc_lics_set = False

    def assert_snippet_exists(self):
        if not self.snippet_spdx_id_set:
            raise OrderError('Snippet')


class Builder(DocBuilder, CreationInfoBuilder, EntityBuilder, ReviewBuilder,
              PackageBuilder, FileBuilder, LicenseBuilder, SnippetBuilder,
              ExternalDocumentRefBuilder, AnnotationBuilder):

    """
    SPDX document builder.
    """

    def __init__(self):
        super(Builder, self).__init__()
        # FIXME: this state does not make sense
        self.reset()

    def reset(self):
        """
        Reset builder's state for building new documents.
        Must be called between usage with different documents.
        """
        # FIXME: this state does not make sense
        self.reset_creation_info()
        self.reset_document()
        self.reset_package()
        self.reset_file_stat()
        self.reset_reviews()
        self.reset_annotations()
        self.reset_extr_lics()
        self.reset_snippet()
