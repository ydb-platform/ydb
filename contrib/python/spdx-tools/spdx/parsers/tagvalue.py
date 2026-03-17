
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

from ply import yacc
import six

from spdx import config
from spdx import utils
from spdx.parsers.builderexceptions import CardinalityError
from spdx.parsers.builderexceptions import OrderError
from spdx.parsers.builderexceptions import SPDXValueError
from spdx.parsers.lexers.tagvalue import Lexer
from spdx import document


ERROR_MESSAGES = {
    'TOOL_VALUE': 'Invalid tool value {0} at line: {1}',
    'ORG_VALUE': 'Invalid organization value {0} at line: {1}',
    'PERSON_VALUE': 'Invalid person value {0} at line: {1}',
    'CREATED_VALUE_TYPE': 'Created value must be date in ISO 8601 format, line: {0}',
    'MORE_THAN_ONE': 'Only one {0} allowed, extra at line: {1}',
    'CREATOR_COMMENT_VALUE_TYPE': 'CreatorComment value must be free form text between <text></text> tags, line:{0}',
    'DOC_LICENSE_VALUE': 'Invalid DataLicense value \'{0}\', line:{1} must be CC0-1.0',
    'DOC_LICENSE_VALUE_TYPE': 'DataLicense must be CC0-1.0, line: {0}',
    'DOC_VERSION_VALUE': 'Invalid SPDXVersion \'{0}\' must be SPDX-M.N where M and N are numbers. Line: {1}',
    'DOC_VERSION_VALUE_TYPE': 'Invalid SPDXVersion value, must be SPDX-M.N where M and N are numbers. Line: {0}',
    'DOC_NAME_VALUE': 'DocumentName must be single line of text, line: {0}',
    'DOC_SPDX_ID_VALUE': 'Invalid SPDXID value, SPDXID must be SPDXRef-DOCUMENT, line: {0}',
    'EXT_DOC_REF_VALUE': 'ExternalDocumentRef must contain External Document ID, SPDX Document URI and Checksum'
                         'in the standard format, line:{0}.',
    'DOC_COMMENT_VALUE_TYPE': 'DocumentComment value must be free form text between <text></text> tags, line:{0}',
    'DOC_NAMESPACE_VALUE': 'Invalid DocumentNamespace value {0}, must contain a scheme (e.g. "https:") '
                           'and should not contain the "#" delimiter, line:{1}',
    'DOC_NAMESPACE_VALUE_TYPE': 'Invalid DocumentNamespace value, must contain a scheme (e.g. "https:") '
                                'and should not contain the "#" delimiter, line: {0}',
    'REVIEWER_VALUE_TYPE': 'Invalid Reviewer value must be a Person, Organization or Tool. Line: {0}',
    'CREATOR_VALUE_TYPE': 'Invalid Reviewer value must be a Person, Organization or Tool. Line: {0}',
    'REVIEW_DATE_VALUE_TYPE': 'ReviewDate value must be date in ISO 8601 format, line: {0}',
    'REVIEW_COMMENT_VALUE_TYPE': 'ReviewComment value must be free form text between <text></text> tags, line:{0}',
    'ANNOTATOR_VALUE_TYPE': 'Invalid Annotator value must be a Person, Organization or Tool. Line: {0}',
    'ANNOTATION_DATE_VALUE_TYPE': 'AnnotationDate value must be date in ISO 8601 format, line: {0}',
    'ANNOTATION_COMMENT_VALUE_TYPE': 'AnnotationComment value must be free form text between <text></text> tags, line:{0}',
    'ANNOTATION_TYPE_VALUE': 'AnnotationType must be "REVIEW" or "OTHER". Line: {0}',
    'ANNOTATION_SPDX_ID_VALUE': 'SPDXREF must be ["DocumentRef-"[idstring]":"]SPDXID where'
                                '["DocumentRef-"[idstring]":"] is an optional reference to an external SPDX document and'
                                'SPDXID is a unique string containing letters, numbers, ".","-".',
    'A_BEFORE_B': '{0} Can not appear before {1}, line: {2}',
    'PACKAGE_NAME_VALUE': 'PackageName must be single line of text, line: {0}',
    'PKG_SPDX_ID_VALUE': 'SPDXID must be "SPDXRef-[idstring]" where [idstring] is a unique string containing '
                             'letters, numbers, ".", "-".',
    'PKG_VERSION_VALUE': 'PackageVersion must be single line of text, line: {0}',
    'PKG_FILE_NAME_VALUE': 'PackageFileName must be single line of text, line: {0}',
    'PKG_SUPPL_VALUE': 'PackageSupplier must be Organization, Person or NOASSERTION, line: {0}',
    'PKG_ORIG_VALUE': 'PackageOriginator must be Organization, Person or NOASSERTION, line: {0}',
    'PKG_DOWN_VALUE': 'PackageDownloadLocation must be a url or NONE or NOASSERTION, line: {0}',
    'PKG_FILES_ANALYZED_VALUE': 'FilesAnalyzed must be a boolean value, line: {0}',
    'PKG_HOME_VALUE': 'PackageHomePage must be a url or NONE or NOASSERTION, line: {0}',
    'PKG_SRC_INFO_VALUE': 'PackageSourceInfo must be free form text, line: {0}',
    'PKG_CHKSUM_VALUE': 'PackageChecksum must be a single line of text, line: {0}',
    'PKG_LICS_CONC_VALUE': 'PackageLicenseConcluded must be NOASSERTION, NONE, license identifier or license list, line: {0}',
    'PKG_LIC_FFILE_VALUE': 'PackageLicenseInfoFromFiles must be, line: {0}',
    'PKG_LICS_DECL_VALUE': 'PackageLicenseDeclared must be NOASSERTION, NONE, license identifier or license list, line: {0}',
    'PKG_LICS_COMMENT_VALUE': 'PackageLicenseComments must be free form text, line: {0}',
    'PKG_SUM_VALUE': 'PackageSummary must be free form text, line: {0}',
    'PKG_DESC_VALUE': 'PackageDescription must be free form text, line: {0}',
    'PKG_COMMENT_VALUE': 'PackageComment must be free form text, line: {0}',
    'PKG_EXT_REF_VALUE': 'ExternalRef must contain category, type, and locator in the standard format, line:{0}.',
    'PKG_EXT_REF_COMMENT_VALUE' : 'ExternalRefComment must be free form text, line:{0}',
    'FILE_NAME_VALUE': 'FileName must be a single line of text, line: {0}',
    'FILE_COMMENT_VALUE': 'FileComment must be free form text, line:{0}',
    'FILE_TYPE_VALUE': 'FileType must be one of OTHER, BINARY, SOURCE or ARCHIVE, line: {0}',
    'FILE_SPDX_ID_VALUE': 'SPDXID must be "SPDXRef-[idstring]" where [idstring] is a unique string containing '
                          'letters, numbers, ".", "-".',
    'FILE_CHKSUM_VALUE': 'FileChecksum must be a single line of text starting with \'SHA1:\', line:{0}',
    'FILE_LICS_CONC_VALUE': 'LicenseConcluded must be NOASSERTION, NONE, license identifier or license list, line:{0}',
    'FILE_LICS_INFO_VALUE': 'LicenseInfoInFile must be NOASSERTION, NONE or license identifier, line: {0}',
    'FILE_LICS_COMMENT_VALUE': 'LicenseComments must be free form lext, line: {0}',
    'FILE_CR_TEXT_VALUE': 'FileCopyrightText must be one of NOASSERTION, NONE or free form text, line: {0}',
    'FILE_NOTICE_VALUE': 'FileNotice must be free form text, line: {0}',
    'FILE_CONTRIB_VALUE': 'FileContributor must be a single line, line: {0}',
    'FILE_DEP_VALUE': 'FileDependency must be a single line, line: {0}',
    'ART_PRJ_NAME_VALUE' : 'ArtifactOfProjectName must be a single line, line: {0}',
    'FILE_ART_OPT_ORDER' : 'ArtificatOfProjectHomePage and ArtificatOfProjectURI must immediatly follow ArtifactOfProjectName, line: {0}',
    'ART_PRJ_HOME_VALUE' : 'ArtificatOfProjectHomePage must be a URL or UNKNOWN, line: {0}',
    'ART_PRJ_URI_VALUE' : 'ArtificatOfProjectURI must be a URI or UNKNOWN, line: {0}',
    'UNKNOWN_TAG' : 'Found unknown tag : {0} at line: {1}',
    'LICS_ID_VALUE' : 'LicenseID must start with \'LicenseRef-\', line: {0}',
    'LICS_TEXT_VALUE' : 'ExtractedText must be free form text, line: {0}',
    'LICS_NAME_VALE' : 'LicenseName must be single line of text or NOASSERTION, line: {0}',
    'LICS_COMMENT_VALUE' : 'LicenseComment must be free form text, line: {0}',
    'LICS_CRS_REF_VALUE' : 'LicenseCrossReference must be uri as single line of text, line: {0}',
    'PKG_CPY_TEXT_VALUE' : 'Package copyright text must be free form text, line: {0}',
    'SNIP_SPDX_ID_VALUE' : 'SPDXID must be "SPDXRef-[idstring]" where [idstring] is a unique string '
                           'containing letters, numbers, ".", "-".',
    'SNIPPET_NAME_VALUE' : 'SnippetName must be a single line of text, line: {0}',
    'SNIP_COMMENT_VALUE' : 'SnippetComment must be free form text, line: {0}',
    'SNIP_COPYRIGHT_VALUE' : 'SnippetCopyrightText must be one of NOASSERTION, NONE or free form text, line: {0}',
    'SNIP_LICS_COMMENT_VALUE' : 'SnippetLicenseComments must be free form text, line: {0}',
    'SNIP_FILE_SPDXID_VALUE' : 'SnippetFromFileSPDXID must be ["DocumentRef-"[idstring]":"] SPDXID '
                               'where DocumentRef-[idstring]: is an optional reference to an external'
                               'SPDX Document and SPDXID is a string containing letters, '
                               'numbers, ".", "-".',
    'SNIP_LICS_CONC_VALUE': 'SnippetLicenseConcluded must be NOASSERTION, NONE, license identifier '
                            'or license list, line:{0}',
    'SNIP_LICS_INFO_VALUE': 'LicenseInfoInSnippet must be NOASSERTION, NONE or license identifier, line: {0}',
}


class Parser(object):

    def __init__(self, builder, logger):
        self.tokens = Lexer.tokens
        self.builder = builder
        self.logger = logger
        self.error = False
        self.license_list_parser = utils.LicenseListParser()
        self.license_list_parser.build(write_tables=0, debug=0)

    def p_start_1(self, p):
        'start : start attrib '
        pass

    def p_start_2(self, p):
        'start : attrib '
        pass

    def p_attrib(self, p):
        """attrib : spdx_version
                  | spdx_id
                  | data_lics
                  | doc_name
                  | ext_doc_ref
                  | doc_comment
                  | doc_namespace
                  | creator
                  | created
                  | creator_comment
                  | locs_list_ver
                  | reviewer
                  | review_date
                  | review_comment
                  | annotator
                  | annotation_date
                  | annotation_comment
                  | annotation_type
                  | annotation_spdx_id
                  | package_name
                  | package_version
                  | pkg_down_location
                  | pkg_files_analyzed
                  | pkg_home
                  | pkg_summary
                  | pkg_src_info
                  | pkg_file_name
                  | pkg_supplier
                  | pkg_orig
                  | pkg_chksum
                  | pkg_verif
                  | pkg_desc
                  | pkg_comment
                  | pkg_lic_decl
                  | pkg_lic_conc
                  | pkg_lic_ff
                  | pkg_lic_comment
                  | pkg_cr_text
                  | pkg_ext_ref
                  | pkg_ext_ref_comment
                  | file_name
                  | file_type
                  | file_chksum
                  | file_conc
                  | file_lics_info
                  | file_cr_text
                  | file_lics_comment
                  | file_notice
                  | file_comment
                  | file_contrib
                  | file_dep
                  | file_artifact
                  | snip_spdx_id
                  | snip_name
                  | snip_comment
                  | snip_cr_text
                  | snip_lic_comment
                  | snip_file_spdx_id
                  | snip_lics_conc
                  | snip_lics_info
                  | extr_lic_id
                  | extr_lic_text
                  | extr_lic_name
                  | lic_xref
                  | lic_comment
                  | unknown_tag
        """
        pass

    def more_than_one_error(self, tag, line):
        self.error = True
        msg = ERROR_MESSAGES['MORE_THAN_ONE'].format(tag, line)
        self.logger.log(msg)

    def order_error(self, first_tag, second_tag, line):
        """Reports an OrderError. Error message will state that
        first_tag came before second_tag.
        """
        self.error = True
        msg = ERROR_MESSAGES['A_BEFORE_B'].format(first_tag, second_tag, line)
        self.logger.log(msg)

    def p_lic_xref_1(self, p):
        """lic_xref : LICS_CRS_REF LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_lic_xref(self.document, value)
        except OrderError:
            self.order_error('LicenseCrossReference', 'LicenseName', p.lineno(1))

    def p_lic_xref_2(self, p):
        """lic_xref : LICS_CRS_REF error"""
        self.error = True
        msg = ERROR_MESSAGES['LICS_CRS_REF_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_lic_comment_1(self, p):
        """lic_comment : LICS_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_lic_comment(self.document, value)
        except OrderError:
            self.order_error('LicenseComment', 'LicenseID', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('LicenseComment', p.lineno(1))

    def p_lic_comment_2(self, p):
        """lic_comment : LICS_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['LICS_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_extr_lic_name_1(self, p):
        """extr_lic_name : LICS_NAME extr_lic_name_value"""
        try:
            self.builder.set_lic_name(self.document, p[2])
        except OrderError:
            self.order_error('LicenseName', 'LicenseID', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('LicenseName', p.lineno(1))

    def p_extr_lic_name_2(self, p):
        """extr_lic_name : LICS_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['LICS_NAME_VALE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_extr_lic_name_value_1(self, p):
        """extr_lic_name_value : LINE"""
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_extr_lic_name_value_2(self, p):
        """extr_lic_name_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_extr_lic_text_1(self, p):
        """extr_lic_text : LICS_TEXT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_lic_text(self.document, value)
        except OrderError:
            self.order_error('ExtractedText', 'LicenseID', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('ExtractedText', p.lineno(1))

    def p_extr_lic_text_2(self, p):
        """extr_lic_text : LICS_TEXT error"""
        self.error = True
        msg = ERROR_MESSAGES['LICS_TEXT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_extr_lic_id_1(self, p):
        """extr_lic_id : LICS_ID LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_lic_id(self.document, value)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['LICS_ID_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_extr_lic_id_2(self, p):
        """extr_lic_id : LICS_ID error"""
        self.error = True
        msg = ERROR_MESSAGES['LICS_ID_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_uknown_tag(self, p):
        """unknown_tag : UNKNOWN_TAG LINE"""
        self.error = True
        msg = ERROR_MESSAGES['UNKNOWN_TAG'].format(p[1], p.lineno(1))
        self.logger.log(msg)

    def p_file_artifact_1(self, p):
        """file_artifact : prj_name_art file_art_rest
                         | prj_name_art
        """
        pass

    def p_file_artificat_2(self, p):
        """file_artifact : prj_name_art error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_ART_OPT_ORDER'].format(p.lineno(2))
        self.logger.log(msg)

    def p_file_art_rest(self, p):
        """file_art_rest : prj_home_art prj_uri_art
                         | prj_uri_art prj_home_art
                         | prj_home_art
                         | prj_uri_art
        """
        pass

    def p_prj_uri_art_1(self, p):
        """prj_uri_art : ART_PRJ_URI UN_KNOWN"""
        try:
            self.builder.set_file_atrificat_of_project(self.document,
                'uri', utils.UnKnown())
        except OrderError:
            self.order_error('ArtificatOfProjectURI', 'FileName', p.lineno(1))

    def p_prj_uri_art_2(self, p):
        """prj_uri_art : ART_PRJ_URI LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_atrificat_of_project(self.document, 'uri', value)
        except OrderError:
            self.order_error('ArtificatOfProjectURI', 'FileName', p.lineno(1))

    def p_prj_uri_art_3(self, p):
        """prj_uri_art : ART_PRJ_URI error"""
        self.error = True
        msg = ERROR_MESSAGES['ART_PRJ_URI_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_prj_home_art_1(self, p):
        """prj_home_art : ART_PRJ_HOME LINE"""
        try:
            self.builder.set_file_atrificat_of_project(self.document, 'home', p[2])
        except OrderError:
            self.order_error('ArtificatOfProjectHomePage', 'FileName', p.lineno(1))

    def p_prj_home_art_2(self, p):
        """prj_home_art : ART_PRJ_HOME UN_KNOWN"""
        try:
            self.builder.set_file_atrificat_of_project(self.document,
                'home', utils.UnKnown())
        except OrderError:
            self.order_error('ArtifactOfProjectName', 'FileName', p.lineno(1))

    def p_prj_home_art_3(self, p):
        """prj_home_art : ART_PRJ_HOME error"""
        self.error = True
        msg = ERROR_MESSAGES['ART_PRJ_HOME_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_prj_name_art_1(self, p):
        """prj_name_art : ART_PRJ_NAME LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_atrificat_of_project(self.document, 'name', value)
        except OrderError:
            self.order_error('ArtifactOfProjectName', 'FileName', p.lineno(1))

    def p_prj_name_art_2(self, p):
        """prj_name_art : ART_PRJ_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['ART_PRJ_NAME_VALUE'].format(p.lineno())
        self.logger.log(msg)

    def p_file_dep_1(self, p):
        """file_dep : FILE_DEP LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_file_dep(self.document, value)
        except OrderError:
            self.order_error('FileDependency', 'FileName', p.lineno(1))

    def p_file_dep_2(self, p):
        """file_dep : FILE_DEP error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_DEP_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_contrib_1(self, p):
        """file_contrib : FILE_CONTRIB LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_file_contribution(self.document, value)
        except OrderError:
            self.order_error('FileContributor', 'FileName', p.lineno(1))

    def p_file_contrib_2(self, p):
        """file_contrib : FILE_CONTRIB error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_CONTRIB_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_notice_1(self, p):
        """file_notice : FILE_NOTICE TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_notice(self.document, value)
        except OrderError:
            self.order_error('FileNotice', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('FileNotice', p.lineno(1))

    def p_file_notice_2(self, p):
        """file_notice : FILE_NOTICE error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_NOTICE_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_cr_text_1(self, p):
        """file_cr_text : FILE_CR_TEXT file_cr_value"""
        try:
            self.builder.set_file_copyright(self.document, p[2])
        except OrderError:
            self.order_error('FileCopyrightText', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('FileCopyrightText', p.lineno(1))

    def p_file_cr_text_2(self, p):
        """file_cr_text : FILE_CR_TEXT error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_CR_TEXT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_cr_value_1(self, p):
        """file_cr_value : TEXT"""
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_file_cr_value_2(self, p):
        """file_cr_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_file_cr_value_3(self, p):
        """file_cr_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_file_lics_comment_1(self, p):
        """file_lics_comment : FILE_LICS_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_license_comment(self.document, value)
        except OrderError:
            self.order_error('LicenseComments', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('LicenseComments', p.lineno(1))

    def p_file_lics_comment_2(self, p):
        """file_lics_comment : FILE_LICS_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_LICS_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_lics_info_1(self, p):
        """file_lics_info : FILE_LICS_INFO file_lic_info_value"""
        try:
            self.builder.set_file_license_in_file(self.document, p[2])
        except OrderError:
            self.order_error('LicenseInfoInFile', 'FileName', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['FILE_LICS_INFO_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_file_lics_info_2(self, p):
        """file_lics_info : FILE_LICS_INFO error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_LICS_INFO_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_lic_info_value_1(self, p):
        """file_lic_info_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_file_lic_info_value_2(self, p):
        """file_lic_info_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    # License Identifier
    def p_file_lic_info_value_3(self, p):
        """file_lic_info_value : LINE"""
        if six.PY2:
            value = p[1].decode(encoding='utf-8')
        else:
            value = p[1]
        p[0] = document.License.from_identifier(value)

    def p_conc_license_1(self, p):
        """conc_license : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_conc_license_2(self, p):
        """conc_license : NONE"""
        p[0] = utils.SPDXNone()

    def p_conc_license_3(self, p):
        """conc_license : LINE"""
        if six.PY2:
            value = p[1].decode(encoding='utf-8')
        else:
            value = p[1]
        ref_re = re.compile('LicenseRef-.+', re.UNICODE)
        if (p[1] in config.LICENSE_MAP.keys()) or (ref_re.match(p[1]) is not None):
            p[0] = document.License.from_identifier(value)
        else:
            p[0] = self.license_list_parser.parse(value)

    def p_file_name_1(self, p):
        """file_name : FILE_NAME LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_name(self.document, value)
        except OrderError:
            self.order_error('FileName', 'PackageName', p.lineno(1))

    def p_file_name_2(self, p):
        """file_name : FILE_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_NAME_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_spdx_id(self, p):
        """spdx_id : SPDX_ID LINE"""
        if six.PY2:
            value = p[2].decode(encoding='utf-8')
        else:
            value = p[2]
        if not self.builder.doc_spdx_id_set:
            self.builder.set_doc_spdx_id(self.document, value)
        elif not self.builder.package_spdx_id_set:
            self.builder.set_pkg_spdx_id(self.document, value)
        else:
            self.builder.set_file_spdx_id(self.document, value)

    def p_file_comment_1(self, p):
        """file_comment : FILE_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_comment(self.document, value)
        except OrderError:
            self.order_error('FileComment', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('FileComment', p.lineno(1))

    def p_file_comment_2(self, p):
        """file_comment : FILE_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_type_1(self, p):
        """file_type : FILE_TYPE file_type_value"""
        try:
            self.builder.set_file_type(self.document, p[2])
        except OrderError:
            self.order_error('FileType', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('FileType', p.lineno(1))

    def p_file_type_2(self, p):
        """file_type : FILE_TYPE error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_TYPE_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_chksum_1(self, p):
        """file_chksum : FILE_CHKSUM CHKSUM"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_file_chksum(self.document, value)
        except OrderError:
            self.order_error('FileChecksum', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('FileChecksum', p.lineno(1))

    def p_file_chksum_2(self, p):
        """file_chksum : FILE_CHKSUM error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_CHKSUM_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_conc_1(self, p):
        """file_conc : FILE_LICS_CONC conc_license"""
        try:
            self.builder.set_concluded_license(self.document, p[2])
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['FILE_LICS_CONC_VALUE'].format(p.lineno(1))
            self.logger.log(msg)
        except OrderError:
            self.order_error('LicenseConcluded', 'FileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('LicenseConcluded', p.lineno(1))

    def p_file_conc_2(self, p):
        """file_conc : FILE_LICS_CONC error"""
        self.error = True
        msg = ERROR_MESSAGES['FILE_LICS_CONC_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_file_type_value(self, p):
        """file_type_value : OTHER
                           | SOURCE
                           | ARCHIVE
                           | BINARY
        """
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_pkg_desc_1(self, p):
        """pkg_desc : PKG_DESC TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_desc(self.document, value)
        except CardinalityError:
            self.more_than_one_error('PackageDescription', p.lineno(1))
        except OrderError:
            self.order_error('PackageDescription', 'PackageFileName', p.lineno(1))

    def p_pkg_desc_2(self, p):
        """pkg_desc : PKG_DESC error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_DESC_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_comment_1(self, p):
        """pkg_comment : PKG_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_comment(self.document, value)
        except CardinalityError:
            self.more_than_one_error('PackageComment', p.lineno(1))
        except OrderError:
            self.order_error('PackageComment', 'PackageFileName',
                             p.lineno(1))

    def p_pkg_comment_2(self, p):
        """pkg_comment : PKG_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_summary_1(self, p):
        """pkg_summary : PKG_SUM TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_summary(self.document, value)
        except OrderError:
            self.order_error('PackageSummary', 'PackageFileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageSummary', p.lineno(1))

    def p_pkg_summary_2(self, p):
        """pkg_summary : PKG_SUM error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_SUM_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_cr_text_1(self, p):
        """pkg_cr_text : PKG_CPY_TEXT pkg_cr_text_value"""
        try:
            self.builder.set_pkg_cr_text(self.document, p[2])
        except OrderError:
            self.order_error('PackageCopyrightText', 'PackageFileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageCopyrightText', p.lineno(1))

    def p_pkg_cr_text_2(self, p):
        """pkg_cr_text : PKG_CPY_TEXT error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_CPY_TEXT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_ext_refs_1(self, p):
        """pkg_ext_ref : PKG_EXT_REF LINE"""
        try:
            if six.PY2:
                pkg_ext_info = p[2].decode(encoding='utf-8')
            else:
                pkg_ext_info = p[2]
            if len(pkg_ext_info.split()) != 3:
                raise SPDXValueError
            else:
                pkg_ext_category, pkg_ext_type, pkg_ext_locator = pkg_ext_info.split()
            self.builder.add_pkg_ext_refs(self.document, pkg_ext_category,
                                          pkg_ext_type, pkg_ext_locator)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_EXT_REF_VALUE'].format(p.lineno(2))
            self.logger.log(msg)

    def p_pkg_ext_refs_2(self, p):
        """pkg_ext_ref : PKG_EXT_REF error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_EXT_REF_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_ext_ref_comment_1(self, p):
        """pkg_ext_ref_comment : PKG_EXT_REF_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_pkg_ext_ref_comment(self.document, value)
        except CardinalityError:
            self.more_than_one_error('ExternalRefComment', p.lineno(1))

    def p_pkg_ext_ref_comment_2(self, p):
        """pkg_ext_ref_comment : PKG_EXT_REF_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_EXT_REF_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_cr_text_value_1(self, p):
        """pkg_cr_text_value : TEXT"""
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_pkg_cr_text_value_2(self, p):
        """pkg_cr_text_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_pkg_cr_text_value_3(self, p):
        """pkg_cr_text_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_pkg_lic_comment_1(self, p):
        """pkg_lic_comment : PKG_LICS_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_license_comment(self.document, value)
        except OrderError:
            self.order_error('PackageLicenseComments', 'PackageFileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageLicenseComments', p.lineno(1))

    def p_pkg_lic_comment_2(self, p):
        """pkg_lic_comment : PKG_LICS_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_LICS_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_lic_decl_1(self, p):
        """pkg_lic_decl : PKG_LICS_DECL conc_license"""
        try:
            self.builder.set_pkg_license_declared(self.document, p[2])
        except OrderError:
            self.order_error('PackageLicenseDeclared', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageLicenseDeclared', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_LICS_DECL_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_pkg_lic_decl_2(self, p):
        """pkg_lic_decl : PKG_LICS_DECL error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_LICS_DECL_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_lic_ff_1(self, p):
        """pkg_lic_ff : PKG_LICS_FFILE pkg_lic_ff_value"""
        try:
            self.builder.set_pkg_license_from_file(self.document, p[2])
        except OrderError:
            self.order_error('PackageLicenseInfoFromFiles', 'PackageName', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_LIC_FFILE_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_pkg_lic_ff_value_1(self, p):
        """pkg_lic_ff_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_pkg_lic_ff_value_2(self, p):
        """pkg_lic_ff_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_pkg_lic_ff_value_3(self, p):
        """pkg_lic_ff_value : LINE"""
        if six.PY2:
            value = p[1].decode(encoding='utf-8')
        else:
            value = p[1]
        p[0] = document.License.from_identifier(value)

    def p_pkg_lic_ff_2(self, p):
        """pkg_lic_ff : PKG_LICS_FFILE error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_LIC_FFILE_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_lic_conc_1(self, p):
        """pkg_lic_conc : PKG_LICS_CONC conc_license"""
        try:
            self.builder.set_pkg_licenses_concluded(self.document, p[2])
        except CardinalityError:
            self.more_than_one_error('PackageLicenseConcluded', p.lineno(1))
        except OrderError:
            self.order_error('PackageLicenseConcluded', 'PackageFileName', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_LICS_CONC_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_pkg_lic_conc_2(self, p):
        """pkg_lic_conc : PKG_LICS_CONC error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_LICS_CONC_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_src_info_1(self, p):
        """pkg_src_info : PKG_SRC_INFO TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_source_info(self.document, value)
        except CardinalityError:
            self.more_than_one_error('PackageSourceInfo', p.lineno(1))
        except OrderError:
            self.order_error('PackageSourceInfo', 'PackageFileName', p.lineno(1))

    def p_pkg_src_info_2(self, p):
        """pkg_src_info : PKG_SRC_INFO error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_SRC_INFO_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_chksum_1(self, p):
        """pkg_chksum : PKG_CHKSUM CHKSUM"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_chk_sum(self.document, value)
        except OrderError:
            self.order_error('PackageChecksum', 'PackageFileName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageChecksum', p.lineno(1))

    def p_pkg_chksum_2(self, p):
        """pkg_chksum : PKG_CHKSUM error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_CHKSUM_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_verif_1(self, p):
        """pkg_verif : PKG_VERF_CODE LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_verif_code(self.document, value)
        except OrderError:
            self.order_error('PackageVerificationCode', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageVerificationCode', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_VERF_CODE_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_pkg_verif_2(self, p):
        """pkg_verif : PKG_VERF_CODE error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_VERF_CODE_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_home_1(self, p):
        """pkg_home : PKG_HOME pkg_home_value"""
        try:
            self.builder.set_pkg_home(self.document, p[2])
        except OrderError:
            self.order_error('PackageHomePage', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageHomePage', p.lineno(1))

    def p_pkg_home_2(self, p):
        """pkg_home : PKG_HOME error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_HOME_VALUE']
        self.logger.log(msg)

    def p_pkg_home_value_1(self, p):
        """pkg_home_value : LINE"""
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_pkg_home_value_2(self, p):
        """pkg_home_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_pkg_home_value_3(self, p):
        """pkg_home_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_pkg_down_location_1(self, p):
        """pkg_down_location : PKG_DOWN pkg_down_value"""
        try:
            self.builder.set_pkg_down_location(self.document, p[2])
        except OrderError:
            self.order_error('PackageDownloadLocation', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageDownloadLocation', p.lineno(1))

    def p_pkg_down_location_2(self, p):
        """pkg_down_location : PKG_DOWN error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_DOWN_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_files_analyzed_1(self, p):
        """pkg_files_analyzed : PKG_FILES_ANALYZED LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_files_analyzed(self.document, value)
        except CardinalityError:
            self.more_than_one_error('FilesAnalyzed', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_FILES_ANALYZED_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_pkg_files_analyzed_2(self, p):
        """pkg_files_analyzed : PKG_FILES_ANALYZED error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_FILES_ANALYZED_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_down_value_1(self, p):
        """pkg_down_value : LINE """
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_pkg_down_value_2(self, p):
        """pkg_down_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_pkg_down_value_3(self, p):
        """pkg_down_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_pkg_orig_1(self, p):
        """pkg_orig : PKG_ORIG pkg_supplier_values"""
        try:
            self.builder.set_pkg_originator(self.document, p[2])
        except OrderError:
            self.order_error('PackageOriginator', 'PackageName', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_ORIG_VALUE'].format(p.lineno(1))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('PackageOriginator', p.lineno(1))

    def p_pkg_orig_2(self, p):
        """pkg_orig : PKG_ORIG error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_ORIG_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_supplier_1(self, p):
        """pkg_supplier : PKG_SUPPL pkg_supplier_values"""
        try:
            self.builder.set_pkg_supplier(self.document, p[2])
        except OrderError:
            self.order_error('PackageSupplier', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageSupplier', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['PKG_SUPPL_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_pkg_supplier_2(self, p):
        """pkg_supplier : PKG_SUPPL error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_SUPPL_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_pkg_supplier_values_1(self, p):
        """pkg_supplier_values : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_pkg_supplier_values_2(self, p):
        """pkg_supplier_values : entity"""
        p[0] = p[1]

    def p_pkg_file_name(self, p):
        """pkg_file_name : PKG_FILE_NAME LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_file_name(self.document, value)
        except OrderError:
            self.order_error('PackageFileName', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageFileName', p.lineno(1))

    def p_pkg_file_name_1(self, p):
        """pkg_file_name : PKG_FILE_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_FILE_NAME_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_package_version_1(self, p):
        """package_version : PKG_VERSION LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_pkg_vers(self.document, value)
        except OrderError:
            self.order_error('PackageVersion', 'PackageName', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('PackageVersion', p.lineno(1))

    def p_package_version_2(self, p):
        """package_version : PKG_VERSION error"""
        self.error = True
        msg = ERROR_MESSAGES['PKG_VERSION_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_package_name(self, p):
        """package_name : PKG_NAME LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.create_package(self.document, value)
        except CardinalityError:
            self.more_than_one_error('PackageName', p.lineno(1))

    def p_package_name_1(self, p):
        """package_name : PKG_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['PACKAGE_NAME_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snip_spdx_id(self, p):
        """snip_spdx_id : SNIPPET_SPDX_ID LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.create_snippet(self.document, value)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_SPDX_ID_VALUE'].format(p.lineno(2))
            self.logger.log(msg)

    def p_snip_spdx_id_1(self, p):
        """snip_spdx_id : SNIPPET_SPDX_ID error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_SPDX_ID_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snippet_name(self, p):
        """snip_name : SNIPPET_NAME LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_snippet_name(self.document, value)
        except OrderError:
            self.order_error('SnippetName', 'SnippetSPDXID', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('SnippetName', p.lineno(1))

    def p_snippet_name_1(self, p):
        """snip_name : SNIPPET_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIPPET_NAME_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snippet_comment(self, p):
        """snip_comment : SNIPPET_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_snippet_comment(self.document, value)
        except OrderError:
            self.order_error('SnippetComment', 'SnippetSPDXID', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_COMMENT_VALUE'].format(p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('SnippetComment', p.lineno(1))

    def p_snippet_comment_1(self, p):
        """snip_comment : SNIPPET_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snippet_cr_text(self, p):
        """snip_cr_text : SNIPPET_CR_TEXT snip_cr_value"""
        try:
            self.builder.set_snippet_copyright(self.document, p[2])
        except OrderError:
            self.order_error('SnippetCopyrightText', 'SnippetSPDXID', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_COPYRIGHT_VALUE'].format(p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('SnippetCopyrightText', p.lineno(1))

    def p_snippet_cr_text_1(self, p):
        """snip_cr_text : SNIPPET_CR_TEXT error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_COPYRIGHT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snippet_cr_value_1(self, p):
        """snip_cr_value : TEXT"""
        if six.PY2:
            p[0] = p[1].decode(encoding='utf-8')
        else:
            p[0] = p[1]

    def p_snippet_cr_value_2(self, p):
        """snip_cr_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_snippet_cr_value_3(self, p):
        """snip_cr_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_snippet_lic_comment(self, p):
        """snip_lic_comment : SNIPPET_LICS_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_snippet_lic_comment(self.document, value)
        except OrderError:
            self.order_error('SnippetLicenseComments', 'SnippetSPDXID', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_LICS_COMMENT_VALUE'].format(p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('SnippetLicenseComments', p.lineno(1))

    def p_snippet_lic_comment_1(self, p):
        """snip_lic_comment : SNIPPET_LICS_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_LICS_COMMENT_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snip_from_file_spdxid(self, p):
        """snip_file_spdx_id : SNIPPET_FILE_SPDXID LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_snip_from_file_spdxid(self.document, value)
        except OrderError:
            self.order_error('SnippetFromFileSPDXID', 'SnippetSPDXID', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_FILE_SPDXID_VALUE'].format(p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('SnippetFromFileSPDXID', p.lineno(1))

    def p_snip_from_file_spdxid_1(self, p):
        """snip_file_spdx_id : SNIPPET_FILE_SPDXID error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_FILE_SPDXID_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snippet_concluded_license(self, p):
        """snip_lics_conc : SNIPPET_LICS_CONC conc_license"""
        try:
            self.builder.set_snip_concluded_license(self.document, p[2])
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_LICS_CONC_VALUE'].format(p.lineno(1))
            self.logger.log(msg)
        except OrderError:
            self.order_error('SnippetLicenseConcluded',
                             'SnippetSPDXID', p.lineno(1))
        except CardinalityError:
            self.more_than_one_error('SnippetLicenseConcluded', p.lineno(1))

    def p_snippet_concluded_license_1(self, p):
        """snip_lics_conc : SNIPPET_LICS_CONC error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_LICS_CONC_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snippet_lics_info(self, p):
        """snip_lics_info : SNIPPET_LICS_INFO snip_lic_info_value"""
        try:
            self.builder.set_snippet_lics_info(self.document, p[2])
        except OrderError:
            self.order_error(
                'LicenseInfoInSnippet', 'SnippetSPDXID', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['SNIP_LICS_INFO_VALUE'].format(p.lineno(1))
            self.logger.log(msg)

    def p_snippet_lics_info_1(self, p):
        """snip_lics_info : SNIPPET_LICS_INFO error"""
        self.error = True
        msg = ERROR_MESSAGES['SNIP_LICS_INFO_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_snip_lic_info_value_1(self, p):
        """snip_lic_info_value : NONE"""
        p[0] = utils.SPDXNone()

    def p_snip_lic_info_value_2(self, p):
        """snip_lic_info_value : NO_ASSERT"""
        p[0] = utils.NoAssert()

    def p_snip_lic_info_value_3(self, p):
        """snip_lic_info_value : LINE"""
        if six.PY2:
            value = p[1].decode(encoding='utf-8')
        else:
            value = p[1]
        p[0] = document.License.from_identifier(value)

    def p_reviewer_1(self, p):
        """reviewer : REVIEWER entity"""
        self.builder.add_reviewer(self.document, p[2])

    def p_reviewer_2(self, p):
        """reviewer : REVIEWER error"""
        self.error = True
        msg = ERROR_MESSAGES['REVIEWER_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_review_date_1(self, p):
        """review_date : REVIEW_DATE DATE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_review_date(self.document, value)
        except CardinalityError:
            self.more_than_one_error('ReviewDate', p.lineno(1))
        except OrderError:
            self.order_error('ReviewDate', 'Reviewer', p.lineno(1))

    def p_review_date_2(self, p):
        """review_date : REVIEW_DATE error"""
        self.error = True
        msg = ERROR_MESSAGES['REVIEW_DATE_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_review_comment_1(self, p):
        """review_comment : REVIEW_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_review_comment(self.document, value)
        except CardinalityError:
            self.more_than_one_error('ReviewComment', p.lineno(1))
        except OrderError:
            self.order_error('ReviewComment', 'Reviewer', p.lineno(1))

    def p_review_comment_2(self, p):
        """review_comment : REVIEW_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['REVIEW_COMMENT_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_annotator_1(self, p):
        """annotator : ANNOTATOR entity"""
        self.builder.add_annotator(self.document, p[2])

    def p_annotator_2(self, p):
        """annotator : ANNOTATOR error"""
        self.error = True
        msg = ERROR_MESSAGES['ANNOTATOR_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_annotation_date_1(self, p):
        """annotation_date : ANNOTATION_DATE DATE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_annotation_date(self.document, value)
        except CardinalityError:
            self.more_than_one_error('AnnotationDate', p.lineno(1))
        except OrderError:
            self.order_error('AnnotationDate', 'Annotator', p.lineno(1))

    def p_annotation_date_2(self, p):
        """annotation_date : ANNOTATION_DATE error"""
        self.error = True
        msg = ERROR_MESSAGES['ANNOTATION_DATE_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_annotation_comment_1(self, p):
        """annotation_comment : ANNOTATION_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_annotation_comment(self.document, value)
        except CardinalityError:
            self.more_than_one_error('AnnotationComment', p.lineno(1))
        except OrderError:
            self.order_error('AnnotationComment', 'Annotator', p.lineno(1))

    def p_annotation_comment_2(self, p):
        """annotation_comment : ANNOTATION_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['ANNOTATION_COMMENT_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_annotation_type_1(self, p):
        """annotation_type : ANNOTATION_TYPE LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.add_annotation_type(self.document, value)
        except CardinalityError:
            self.more_than_one_error('AnnotationType', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['ANNOTATION_TYPE_VALUE'].format(p.lineno(1))
            self.logger.log(msg)
        except OrderError:
            self.order_error('AnnotationType', 'Annotator', p.lineno(1))

    def p_annotation_type_2(self, p):
        """annotation_type : ANNOTATION_TYPE error"""
        self.error = True
        msg = ERROR_MESSAGES['ANNOTATION_TYPE_VALUE'].format(
            p.lineno(1))
        self.logger.log(msg)

    def p_annotation_spdx_id_1(self, p):
        """annotation_spdx_id : ANNOTATION_SPDX_ID LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_annotation_spdx_id(self.document, value)
        except CardinalityError:
            self.more_than_one_error('SPDXREF', p.lineno(1))
        except OrderError:
            self.order_error('SPDXREF', 'Annotator', p.lineno(1))

    def p_annotation_spdx_id_2(self, p):
        """annotation_spdx_id : ANNOTATION_SPDX_ID error"""
        self.error = True
        msg = ERROR_MESSAGES['ANNOTATION_SPDX_ID_VALUE'].format(
            p.lineno(1))
        self.logger.log(msg)

    def p_lics_list_ver_1(self, p):
        """locs_list_ver : LIC_LIST_VER LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_lics_list_ver(self.document, value)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['LIC_LIST_VER_VALUE'].format(
                p[2], p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('LicenseListVersion', p.lineno(1))

    def p_lics_list_ver_2(self, p):
        """locs_list_ver : LIC_LIST_VER error"""
        self.error = True
        msg = ERROR_MESSAGES['LIC_LIST_VER_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_doc_comment_1(self, p):
        """doc_comment : DOC_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_doc_comment(self.document, value)
        except CardinalityError:
            self.more_than_one_error('DocumentComment', p.lineno(1))

    def p_doc_comment_2(self, p):
        """doc_comment : DOC_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['DOC_COMMENT_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_doc_namespace_1(self, p):
        """doc_namespace : DOC_NAMESPACE LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_doc_namespace(self.document, value)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['DOC_NAMESPACE_VALUE'].format(p[2], p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('DocumentNamespace', p.lineno(1))

    def p_doc_namespace_2(self, p):
        """doc_namespace : DOC_NAMESPACE error"""
        self.error = True
        msg = ERROR_MESSAGES['DOC_NAMESPACE_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_data_license_1(self, p):
        """data_lics : DOC_LICENSE LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_doc_data_lics(self.document, value)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['DOC_LICENSE_VALUE'].format(p[2], p.lineno(2))
            self.logger.log(msg)
        except CardinalityError:
            self.more_than_one_error('DataLicense', p.lineno(1))

    def p_data_license_2(self, p):
        """data_lics : DOC_LICENSE error"""
        self.error = True
        msg = ERROR_MESSAGES['DOC_LICENSE_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_doc_name_1(self, p):
        """doc_name : DOC_NAME LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_doc_name(self.document, value)
        except CardinalityError:
            self.more_than_one_error('DocumentName', p.lineno(1))

    def p_doc_name_2(self, p):
        """doc_name : DOC_NAME error"""
        self.error = True
        msg = ERROR_MESSAGES['DOC_NAME_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_ext_doc_refs_1(self, p):
        """ext_doc_ref : EXT_DOC_REF DOC_REF_ID DOC_URI EXT_DOC_REF_CHKSUM"""
        try:
            if six.PY2:
                doc_ref_id = p[2].decode(encoding='utf-8')
                doc_uri = p[3].decode(encoding='utf-8')
                ext_doc_chksum = p[4].decode(encoding='utf-8')
            else:
                doc_ref_id = p[2]
                doc_uri = p[3]
                ext_doc_chksum = p[4]

            self.builder.add_ext_doc_refs(self.document, doc_ref_id, doc_uri,
                                          ext_doc_chksum)
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['EXT_DOC_REF_VALUE'].format(p.lineno(2))
            self.logger.log(msg)

    def p_ext_doc_refs_2(self, p):
        """ext_doc_ref : EXT_DOC_REF error"""
        self.error = True
        msg = ERROR_MESSAGES['EXT_DOC_REF_VALUE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_spdx_version_1(self, p):
        """spdx_version : DOC_VERSION LINE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_doc_version(self.document, value)
        except CardinalityError:
            self.more_than_one_error('SPDXVersion', p.lineno(1))
        except SPDXValueError:
            self.error = True
            msg = ERROR_MESSAGES['DOC_VERSION_VALUE'].format(p[2], p.lineno(1))
            self.logger.log(msg)

    def p_spdx_version_2(self, p):
        """spdx_version : DOC_VERSION error"""
        self.error = True
        msg = ERROR_MESSAGES['DOC_VERSION_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_creator_comment_1(self, p):
        """creator_comment : CREATOR_COMMENT TEXT"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_creation_comment(self.document, value)
        except CardinalityError:
            self.more_than_one_error('CreatorComment', p.lineno(1))

    def p_creator_comment_2(self, p):
        """creator_comment : CREATOR_COMMENT error"""
        self.error = True
        msg = ERROR_MESSAGES['CREATOR_COMMENT_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_creator_1(self, p):
        """creator : CREATOR entity"""
        self.builder.add_creator(self.document, p[2])

    def p_creator_2(self, p):
        """creator : CREATOR error"""
        self.error = True
        msg = ERROR_MESSAGES['CREATOR_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_created_1(self, p):
        """created : CREATED DATE"""
        try:
            if six.PY2:
                value = p[2].decode(encoding='utf-8')
            else:
                value = p[2]
            self.builder.set_created_date(self.document, value)
        except CardinalityError:
            self.more_than_one_error('Created', p.lineno(1))

    def p_created_2(self, p):
        """created : CREATED error"""
        self.error = True
        msg = ERROR_MESSAGES['CREATED_VALUE_TYPE'].format(p.lineno(1))
        self.logger.log(msg)

    def p_entity_1(self, p):
        """entity : TOOL_VALUE
        """
        try:
            if six.PY2:
                value = p[1].decode(encoding='utf-8')
            else:
                value = p[1]
            p[0] = self.builder.build_tool(self.document, value)
        except SPDXValueError:
            msg = ERROR_MESSAGES['TOOL_VALUE'].format(p[1], p.lineno(1))
            self.logger.log(msg)
            self.error = True
            p[0] = None

    def p_entity_2(self, p):
        """entity : ORG_VALUE
        """
        try:
            if six.PY2:
                value = p[1].decode(encoding='utf-8')
            else:
                value = p[1]
            p[0] = self.builder.build_org(self.document, value)
        except SPDXValueError:
            msg = ERROR_MESSAGES['ORG_VALUE'].format(p[1], p.lineno(1))
            self.logger.log(msg)
            self.error = True
            p[0] = None

    def p_entity_3(self, p):
        """entity : PERSON_VALUE
        """
        try:
            if six.PY2:
                value = p[1].decode(encoding='utf-8')
            else:
                value = p[1]
            p[0] = self.builder.build_person(self.document, value)
        except SPDXValueError:
            msg = ERROR_MESSAGES['PERSON_VALUE'].format(p[1], p.lineno(1))
            self.logger.log(msg)
            self.error = True
            p[0] = None

    def p_error(self, p):
        pass

    def build(self, **kwargs):
        self.lex = Lexer()
        self.lex.build(reflags=re.UNICODE)
        self.yacc = yacc.yacc(module=self, **kwargs)

    def parse(self, text):
        self.document = document.Document()
        self.error = False
        self.yacc.parse(text, lexer=self.lex)
        # FIXME: this state does not make sense
        self.builder.reset()
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
