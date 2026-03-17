
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

import six
from six.moves import reduce

from rdflib import Graph
from rdflib import Namespace
from rdflib import RDF
from rdflib import RDFS

from spdx import document
from spdx import utils
from spdx.parsers.builderexceptions import CardinalityError
from spdx.parsers.builderexceptions import SPDXValueError


ERROR_MESSAGES = {
    'DOC_VERS_VALUE': 'Invalid specVersion \'{0}\' must be SPDX-M.N where M and N are numbers.',
    'DOC_D_LICS': 'Invalid dataLicense \'{0}\' must be http://spdx.org/licenses/CC0-1.0.',
    'DOC_SPDX_ID_VALUE': 'Invalid SPDXID value, SPDXID must be the document namespace appended '
                         'by "#SPDXRef-DOCUMENT", line: {0}',
    'DOC_NAMESPACE_VALUE': 'Invalid DocumentNamespace value {0}, must contain a scheme (e.g. "https:") '
                           'and should not contain the "#" delimiter.',
    'LL_VALUE': 'Invalid licenseListVersion \'{0}\' must be of the format N.N where N is a number',
    'CREATED_VALUE': 'Invalid created value \'{0}\' must be date in ISO 8601 format.',
    'CREATOR_VALUE': 'Invalid creator value \'{0}\' must be Organization, Tool or Person.',
    'EXT_DOC_REF_VALUE': 'Failed to extract {0} from ExternalDocumentRef.',
    'PKG_SPDX_ID_VALUE': 'SPDXID must be "SPDXRef-[idstring]" where [idstring] is a unique string containing '
                         'letters, numbers, ".", "-".',
    'PKG_SUPPL_VALUE': 'Invalid package supplier value \'{0}\' must be Organization, Person or NOASSERTION.',
    'PKG_ORIGINATOR_VALUE': 'Invalid package supplier value \'{0}\'  must be Organization, Person or NOASSERTION.',
    'PKG_DOWN_LOC': 'Invalid package download location value \'{0}\'  must be a url or NONE or NOASSERTION',
    'PKG_FILES_ANALYZED_VALUE': 'FilesAnalyzed must be a boolean value, line: {0}',
    'PKG_CONC_LIST': 'Package concluded license list must have more than one member',
    'LICS_LIST_MEMBER' : 'Declaritive or Conjunctive license set member must be a license url or identifier',
    'PKG_SINGLE_LICS' : 'Package concluded license must be a license url or spdx:noassertion or spdx:none.',
    'PKG_LICS_INFO_FILES' : 'Package licenseInfoFromFiles must be a license or spdx:none or spdx:noassertion',
    'FILE_SPDX_ID_VALUE': 'SPDXID must be "SPDXRef-[idstring]" where [idstring] is a unique string containing '
                          'letters, numbers, ".", "-".',
    'PKG_EXT_REF_CATEGORY': '\'{0}\' must be "SECURITY", "PACKAGE-MANAGER", or "OTHER".',
    'PKG_EXT_REF_TYPE': '{0} must be a unique string containing letters, numbers, ".", or "-".',
    'FILE_TYPE' : 'File type must be binary, other, source or archive term.',
    'FILE_SINGLE_LICS': 'File concluded license must be a license url or spdx:noassertion or spdx:none.',
    'REVIEWER_VALUE' : 'Invalid reviewer value \'{0}\' must be Organization, Tool or Person.',
    'REVIEW_DATE' : 'Invalid review date value \'{0}\' must be date in ISO 8601 format.',
    'ANNOTATOR_VALUE': 'Invalid annotator value \'{0}\' must be Organization, Tool or Person.',
    'ANNOTATION_DATE': 'Invalid annotation date value \'{0}\' must be date in ISO 8601 format.',
    'SNIPPET_SPDX_ID_VALUE' : 'SPDXID must be "SPDXRef-[idstring]" where [idstring] is a unique string '
                              'containing letters, numbers, ".", "-".',
    'SNIPPET_SINGLE_LICS' : 'Snippet Concluded License must be a license url or spdx:noassertion or spdx:none.',
    'SNIPPET_LIC_INFO' : 'License Information in Snippet must be a license url or a reference '
                         'to the license, denoted by LicenseRef-[idstring] or spdx:noassertion or spdx:none.',
}


class BaseParser(object):
    """
    Base class for all parsers.
    Contains logger, doap_namespace, spdx_namespace and model builder.
    Also provides utility functions used by the deriving parsers.
    """

    def __init__(self, builder, logger):
        self.logger = logger
        self.doap_namespace = Namespace('http://usefulinc.com/ns/doap#')
        self.spdx_namespace = Namespace("http://spdx.org/rdf/terms#")
        self.builder = builder

    def more_than_one_error(self, field):
        """
        Logs a more than one error.
        field is the field/property that has more than one defined.
        """
        msg = 'More than one {0} defined.'.format(field)
        self.logger.log(msg)
        self.error = True

    def value_error(self, key, bad_value):
        """
        Report a value error using ERROR_MESSAGES dict.
        key - key to use for ERROR_MESSAGES.
        bad_value - is passed to format which is called on what key maps to
        in ERROR_MESSAGES.
        """
        msg = ERROR_MESSAGES[key].format(bad_value)
        self.logger.log(msg)
        self.error = True

    def to_special_value(self, value):
        """
        Check if value is a special SPDX value such as
        NONE, NOASSERTION or UNKNOWN if so returns proper model.
        else returns value
        """
        if value == self.spdx_namespace.none:
            return utils.SPDXNone()
        elif value == self.spdx_namespace.noassertion:
            return utils.NoAssert()
        elif value == self.spdx_namespace.unknown:
            return utils.UnKnown()
        else:
            return six.text_type(value)


class LicenseParser(BaseParser):
    """
    Helper class for parsing extracted licenses and license lists.
    """

    LICS_REF_REGEX = re.compile('LicenseRef-.+', re.UNICODE)

    def __init__(self, builder, logger):
        super(LicenseParser, self).__init__(builder, logger)

    def handle_lics(self, lics):
        """
        Return a License from a `lics` license resource.
        """
        # Handle extracted licensing info type.
        if (lics, RDF.type, self.spdx_namespace['ExtractedLicensingInfo']) in self.graph:
            return self.parse_only_extr_license(lics)

        # Assume resource, hence the path separator
        ident_start = lics.rfind('/') + 1
        if ident_start == 0:
            # special values such as spdx:noassertion
            special = self.to_special_value(lics)
            if special == lics:
                if self.LICS_REF_REGEX.match(lics):
                    # Is a license ref i.e LicenseRef-1
                    return document.License.from_identifier(six.text_type(lics))
                else:
                    # Not a known license form
                    raise SPDXValueError('License')
            else:
                # is a special value
                return special
        else:
            # license url
            return document.License.from_identifier(lics[ident_start:])

    def get_extr_license_ident(self, extr_lic):
        """
        Return a license identifier from an ExtractedLicense or None.
        """
        identifier_tripples = list(self.graph.triples((extr_lic, self.spdx_namespace['licenseId'], None)))

        if not identifier_tripples:
            self.error = True
            msg = 'Extracted license must have licenseId property.'
            self.logger.log(msg)
            return

        if len(identifier_tripples) > 1:
            self.more_than_one_error('extracted license identifier_tripples')
            return

        identifier_tripple = identifier_tripples[0]
        _s, _p, identifier = identifier_tripple
        return six.text_type(identifier)

    def get_extr_license_text(self, extr_lic):
        """
        Return extracted text from an ExtractedLicense or None.
        """
        text_tripples = list(self.graph.triples((extr_lic, self.spdx_namespace['extractedText'], None)))
        if not text_tripples:
            self.error = True
            msg = 'Extracted license must have extractedText property'
            self.logger.log(msg)
            return

        if len(text_tripples) > 1:
            self.more_than_one_error('extracted license text')
            return

        text_tripple = text_tripples[0]
        _s, _p, text = text_tripple
        return six.text_type(text)

    def get_extr_lic_name(self, extr_lic):
        """
        Return the license name from an ExtractedLicense or None
        """
        extr_name_list = list(self.graph.triples((extr_lic, self.spdx_namespace['licenseName'], None)))
        if len(extr_name_list) > 1:
            self.more_than_one_error('extracted license name')
            return
        elif len(extr_name_list) == 0:
            return
        return six.text_type(self.to_special_value(extr_name_list[0][2]))

    def get_extr_lics_xref(self, extr_lic):
        """
        Return a list of cross references.
        """
        xrefs = list(self.graph.triples((extr_lic, RDFS.seeAlso, None)))
        return list(map(lambda xref_triple: xref_triple[2], xrefs))

    def get_extr_lics_comment(self, extr_lics):
        """
        Return license comment or None.
        """
        comment_list = list(self.graph.triples(
            (extr_lics, RDFS.comment, None)))
        if len(comment_list) > 1 :
            self.more_than_one_error('extracted license comment')
            return
        elif len(comment_list) == 1:
            return six.text_type(comment_list[0][2])
        else:
            return

    def parse_only_extr_license(self, extr_lic):
        """
        Return an ExtractedLicense object to represent a license object.
        But does not add it to the SPDXDocument model.
        Return None if failed.
        """
        # Grab all possible values
        ident = self.get_extr_license_ident(extr_lic)
        text = self.get_extr_license_text(extr_lic)
        comment = self.get_extr_lics_comment(extr_lic)
        xrefs = self.get_extr_lics_xref(extr_lic)
        name = self.get_extr_lic_name(extr_lic)

        if not ident:
            # Must have identifier
            return

        # Set fields
        # FIXME: the constructor of the license should alwas accept a name
        lic = document.ExtractedLicense(ident)
        if text is not None:
            lic.text = text
        if name is not None:
            lic.full_name = name
        if comment is not None:
            lic.comment = comment
        lic.cross_ref = list(map(lambda x: six.text_type(x), xrefs))
        return lic

    def handle_extracted_license(self, extr_lic):
        """
        Build and return an ExtractedLicense or None.
        Note that this function adds the license to the document.
        """
        lic = self.parse_only_extr_license(extr_lic)
        if lic is not None:
            self.doc.add_extr_lic(lic)
        return lic

    def _handle_license_list(self, lics_set, cls=None):
        """
        Return a license representing a `cls` object (LicenseConjunction
        or LicenseDisjunction) from a list of license resources or None.
        """
        licenses = []
        for _, _, lics_member in self.graph.triples(
            (lics_set, self.spdx_namespace['member'], None)):
            try:
                licenses.append(self.handle_lics(lics_member))
            except CardinalityError:
                self.value_error('LICS_LIST_MEMBER', lics_member)
                break
        if len(licenses) > 1:
            return reduce(lambda a, b: cls(a, b), licenses)
        else:
            self.value_error('PKG_CONC_LIST', '')
            return

    def handle_conjunctive_list(self, lics_set):
        """
        Return a license representing the conjunction from a list of
        license resources or None.
        """
        return self._handle_license_list(lics_set, cls=document.LicenseConjunction)

    def handle_disjunctive_list(self, lics_set):
        """
        Return a license representing the disjunction from a list of
        license resources or None.
        """
        return self._handle_license_list(lics_set, cls=document.LicenseDisjunction)


class PackageParser(LicenseParser):
    """
    Helper class for parsing packages.
    """

    def __init__(self, builder, logger):
        super(PackageParser, self).__init__(builder, logger)

    def parse_package(self, p_term):
        """
        Parse package fields.
        """
        # Check there is a pacakge name
        if not (p_term, self.spdx_namespace['name'], None) in self.graph:
            self.error = True
            self.logger.log('Package must have a name.')
            # Create dummy package so that we may continue parsing the rest of
            # the package fields.
            self.builder.create_package(self.doc, 'dummy_package')
        else:
            for _s, _p, o in self.graph.triples((p_term, self.spdx_namespace['name'], None)):
                try:
                    self.builder.create_package(self.doc, six.text_type(o))
                except CardinalityError:
                    self.more_than_one_error('Package name')
                    break
        # Set SPDXID
        try:
            if p_term.count('#', 0, len(p_term)) == 1:
                pkg_spdx_id = p_term.split('#')[-1]
                self.builder.set_pkg_spdx_id(self.doc, pkg_spdx_id)
            else:
                self.value_error('PKG_SPDX_ID_VALUE', p_term)
        except SPDXValueError:
            self.value_error('PKG_SPDX_ID_VALUE', p_term)

        self.p_pkg_vinfo(p_term, self.spdx_namespace['versionInfo'])
        self.p_pkg_fname(p_term, self.spdx_namespace['packageFileName'])
        self.p_pkg_suppl(p_term, self.spdx_namespace['supplier'])
        self.p_pkg_originator(p_term, self.spdx_namespace['originator'])
        self.p_pkg_down_loc(p_term, self.spdx_namespace['downloadLocation'])
        self.p_pkg_files_analyzed(p_term, self.spdx_namespace['filesAnalyzed'])
        self.p_pkg_homepg(p_term, self.doap_namespace['homepage'])
        self.p_pkg_chk_sum(p_term, self.spdx_namespace['checksum'])
        self.p_pkg_src_info(p_term, self.spdx_namespace['sourceInfo'])
        self.p_pkg_verif_code(p_term, self.spdx_namespace['packageVerificationCode'])
        self.p_pkg_lic_conc(p_term, self.spdx_namespace['licenseConcluded'])
        self.p_pkg_lic_decl(p_term, self.spdx_namespace['licenseDeclared'])
        self.p_pkg_lics_info_from_files(p_term, self.spdx_namespace['licenseInfoFromFiles'])
        self.p_pkg_comments_on_lics(p_term, self.spdx_namespace['licenseComments'])
        self.p_pkg_cr_text(p_term, self.spdx_namespace['copyrightText'])
        self.p_pkg_summary(p_term, self.spdx_namespace['summary'])
        self.p_pkg_descr(p_term, self.spdx_namespace['description'])
        self.p_pkg_comment(p_term, self.spdx_namespace['comment'])

    def p_pkg_cr_text(self, p_term, predicate):
        try:
            for _, _, text in self.graph.triples((p_term, predicate, None)):
                self.builder.set_pkg_cr_text(self.doc, six.text_type(self.to_special_value(text)))
        except CardinalityError:
            self.more_than_one_error('package copyright text')

    def p_pkg_summary(self, p_term, predicate):
        try:
            for _, _, summary in self.graph.triples((p_term, predicate, None)):
                self.builder.set_pkg_summary(self.doc, six.text_type(summary))
        except CardinalityError:
            self.more_than_one_error('package summary')

    def p_pkg_descr(self, p_term, predicate):
        try:
            for _, _, desc in self.graph.triples(
                (p_term, predicate, None)):
                self.builder.set_pkg_desc(self.doc, six.text_type(desc))
        except CardinalityError:
            self.more_than_one_error('package description')

    def p_pkg_comment(self, p_term, predicate):
        try:
            for _, _, comment in self.graph.triples((p_term, predicate, None)):
                self.builder.set_pkg_comment(self.doc, six.text_type(comment))
        except CardinalityError:
            self.more_than_one_error('package comment')

    def p_pkg_comments_on_lics(self, p_term, predicate):
        for _, _, comment in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_license_comment(self.doc, six.text_type(comment))
            except CardinalityError:
                self.more_than_one_error('package comments on license')
                break

    def p_pkg_lics_info_from_files(self, p_term, predicate):
        for _, _, lics in self.graph.triples((p_term, predicate, None)):
            try:
                if (lics, RDF.type, self.spdx_namespace['ExtractedLicensingInfo']) in self.graph:
                    self.builder.set_pkg_license_from_file(self.doc, self.parse_only_extr_license(lics))
                else:
                    self.builder.set_pkg_license_from_file(self.doc, self.handle_lics(lics))

            except SPDXValueError:
                self.value_error('PKG_LICS_INFO_FILES', lics)

    def p_pkg_lic_decl(self, p_term, predicate):
        self.handle_pkg_lic(p_term, predicate, self.builder.set_pkg_license_declared)

    def handle_pkg_lic(self, p_term, predicate, builder_func):
        """
        Handle package lics concluded or declared.
        """
        try:
            for _, _, licenses in self.graph.triples((p_term, predicate, None)):
                if (licenses, RDF.type, self.spdx_namespace['ConjunctiveLicenseSet']) in self.graph:
                    lics = self.handle_conjunctive_list(licenses)
                    builder_func(self.doc, lics)

                elif (licenses, RDF.type, self.spdx_namespace['DisjunctiveLicenseSet']) in self.graph:
                    lics = self.handle_disjunctive_list(licenses)
                    builder_func(self.doc, lics)

                else:
                    try:
                        lics = self.handle_lics(licenses)
                        builder_func(self.doc, lics)
                    except SPDXValueError:
                        self.value_error('PKG_SINGLE_LICS', licenses)
        except CardinalityError:
            self.more_than_one_error('package {0}'.format(predicate))

    def p_pkg_lic_conc(self, p_term, predicate):
        self.handle_pkg_lic(p_term, predicate, self.builder.set_pkg_licenses_concluded)

    def p_pkg_verif_code(self, p_term, predicate):
        for _, _, verifcode in self.graph.triples((p_term, predicate, None)):
            # Parse verification code
            for _, _, code in self.graph.triples((verifcode, self.spdx_namespace['packageVerificationCodeValue'], None)):
                try:
                    self.builder.set_pkg_verif_code(self.doc, six.text_type(code))
                except CardinalityError:
                    self.more_than_one_error('package verificaton code')
                    break
            # Parse excluded file
            for _, _, filename in self.graph.triples((verifcode, self.spdx_namespace['packageVerificationCodeExcludedFile'], None)):
                try:
                    self.builder.set_pkg_excl_file(self.doc, six.text_type(filename))
                except CardinalityError:
                    self.more_than_one_error('package verificaton code excluded file')
                    break

    def p_pkg_src_info(self, p_term, predicate):
        for _, _, o in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_source_info(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('package source info')
                break

    def p_pkg_chk_sum(self, p_term, predicate):
        for _s, _p, checksum in self.graph.triples((p_term, predicate, None)):
            for _, _, value in self.graph.triples((checksum, self.spdx_namespace['checksumValue'], None)):
                try:
                    self.builder.set_pkg_chk_sum(self.doc, six.text_type(value))
                except CardinalityError:
                    self.more_than_one_error('Package checksum')
                    break

    def p_pkg_homepg(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_home(self.doc, six.text_type(self.to_special_value(o)))
            except CardinalityError:
                self.more_than_one_error('Package home page')
                break
            except SPDXValueError:
                self.value_error('PKG_HOME_PAGE', o)

    def p_pkg_down_loc(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_down_location(self.doc, six.text_type(self.to_special_value(o)))
            except CardinalityError:
                self.more_than_one_error('Package download location')
                break
            except SPDXValueError:
                self.value_error('PKG_DOWN_LOC', o)

    def p_pkg_files_analyzed(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_files_analyzed(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('Package Files Analyzed')
                break
            except SPDXValueError:
                self.value_error('PKG_FILES_ANALYZED_VALUE', o)

    def p_pkg_originator(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                if o == "NOASSERTION":
                    self.builder.set_pkg_originator(self.doc, utils.NoAssert())
                else:
                    ent = self.builder.create_entity(self.doc, six.text_type(o))
                    self.builder.set_pkg_originator(self.doc, ent)
            except CardinalityError:
                self.more_than_one_error('Package originator')
                break
            except SPDXValueError:
                self.value_error('PKG_ORIGINATOR_VALUE', o)

    def p_pkg_suppl(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                if o == "NOASSERTION":
                    self.builder.set_pkg_supplier(self.doc, utils.NoAssert())
                else:
                    ent = self.builder.create_entity(self.doc, six.text_type(o))
                    self.builder.set_pkg_supplier(self.doc, ent)
            except CardinalityError:
                self.more_than_one_error('Package supplier')
                break
            except SPDXValueError:
                self.value_error('PKG_SUPPL_VALUE', o)

    def p_pkg_fname(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_file_name(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('Package file name')
                break

    def p_pkg_vinfo(self, p_term, predicate):
        for _s, _p, o in self.graph.triples((p_term, predicate, None)):
            try:
                self.builder.set_pkg_vers(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('Package version info')
                break


class FileParser(LicenseParser):
    """
    Helper class for parsing files.
    """

    def __init__(self, builder, logger):
        super(FileParser, self).__init__(builder, logger)

    def parse_file(self, f_term):
        if not (f_term, self.spdx_namespace['fileName'], None) in self.graph:
            self.error = True
            self.logger.log('File must have a name.')
            # Dummy name to continue
            self.builder.set_file_name(self.doc, 'Dummy file')
        else:
            for _, _, name in self.graph.triples((f_term, self.spdx_namespace['fileName'], None)):
                self.builder.set_file_name(self.doc, six.text_type(name))

        self.p_file_spdx_id(f_term, self.spdx_namespace['File'])
        self.p_file_type(f_term, self.spdx_namespace['fileType'])
        self.p_file_chk_sum(f_term, self.spdx_namespace['checksum'])
        self.p_file_lic_conc(f_term, self.spdx_namespace['licenseConcluded'])
        self.p_file_lic_info(f_term, self.spdx_namespace['licenseInfoInFile'])
        self.p_file_comments_on_lics(f_term, self.spdx_namespace['licenseComments'])
        self.p_file_cr_text(f_term, self.spdx_namespace['copyrightText'])
        self.p_file_artifact(f_term, self.spdx_namespace['artifactOf'])
        self.p_file_comment(f_term, RDFS.comment)
        self.p_file_notice(f_term, self.spdx_namespace['noticeText'])
        self.p_file_contributor(f_term, self.spdx_namespace['fileContributor'])
        self.p_file_depends(f_term, self.spdx_namespace['fileDependency'])

    def get_file_name(self, f_term):
        """Returns first found fileName property or None if not found."""
        for _, _, name in self.graph.triples((f_term, self.spdx_namespace['fileName'], None)):
            return name
        return

    def p_file_depends(self, f_term, predicate):
        """
        Set file dependencies.
        """
        for _, _, other_file in self.graph.triples((f_term, predicate, None)):
            name = self.get_file_name(other_file)
            if name is not None:
                self.builder.add_file_dep(six.text_type(name))
            else:
                self.error = True
                msg = 'File depends on file with no name'
                self.logger.log(msg)

    def p_file_contributor(self, f_term, predicate):
        """
        Parse all file contributors and adds them to the model.
        """
        for _, _, contributor in self.graph.triples((f_term, predicate, None)):
            self.builder.add_file_contribution(self.doc, six.text_type(contributor))

    def p_file_notice(self, f_term, predicate):
        """
        Set file notice text.
        """
        try:
            for _, _, notice in self.graph.triples((f_term, predicate, None)):
                self.builder.set_file_notice(self.doc, six.text_type(notice))
        except CardinalityError:
            self.more_than_one_error('file notice')

    def p_file_comment(self, f_term, predicate):
        """
        Set file comment text.
        """
        try:
            for _, _, comment in self.graph.triples((f_term, predicate, None)):
                self.builder.set_file_comment(self.doc, six.text_type(comment))
        except CardinalityError:
            self.more_than_one_error('file comment')


    def p_file_artifact(self, f_term, predicate):
        """
        Handle file artifactOf.
        Note: does not handle artifact of project URI.
        """
        for _, _, project in self.graph.triples((f_term, predicate, None)):
            if (project, RDF.type, self.doap_namespace['Project']):
                self.p_file_project(project)
            else:
                self.error = True
                msg = 'File must be artifact of doap:Project'
                self.logger.log(msg)

    def p_file_project(self, project):
        """
        Helper function for parsing doap:project name and homepage.
        and setting them using the file builder.
        """
        for _, _, name in self.graph.triples((project, self.doap_namespace['name'], None)):
            self.builder.set_file_atrificat_of_project(self.doc, 'name', six.text_type(name))
        for _, _, homepage in self.graph.triples(
            (project, self.doap_namespace['homepage'], None)):
            self.builder.set_file_atrificat_of_project(self.doc, 'home', six.text_type(homepage))

    def p_file_cr_text(self, f_term, predicate):
        """
        Set file copyright text.
        """
        try:
            for _, _, cr_text in self.graph.triples((f_term, predicate, None)):
                self.builder.set_file_copyright(self.doc, six.text_type(cr_text))
        except CardinalityError:
            self.more_than_one_error('file copyright text')

    def p_file_comments_on_lics(self, f_term, predicate):
        """
        Set file license comment.
        """
        try:
            for _, _, comment in self.graph.triples((f_term, predicate, None)):
                self.builder.set_file_license_comment(self.doc, six.text_type(comment))
        except CardinalityError:
            self.more_than_one_error('file comments on license')

    def p_file_lic_info(self, f_term, predicate):
        """
        Set file license information.
        """
        for _, _, info in self.graph.triples((f_term, predicate, None)):
            lic = self.handle_lics(info)
            if lic is not None:
                self.builder.set_file_license_in_file(self.doc, lic)

    def p_file_spdx_id(self, f_term, predicate):
        try:
            try:
                self.builder.set_file_spdx_id(self.doc, six.text_type(f_term))
            except SPDXValueError:
                self.value_error('FILE_SPDX_ID_VALUE', f_term)
        except CardinalityError:
            self.more_than_one_error('FILE_SPDX_ID_VALUE')

    def p_file_type(self, f_term, predicate):
        """
        Set file type.
        """
        try:
            for _, _, ftype in self.graph.triples((f_term, predicate, None)):
                try:
                    if ftype.endswith('binary'):
                        ftype = 'BINARY'
                    elif ftype.endswith('source'):
                        ftype = 'SOURCE'
                    elif ftype.endswith('other'):
                        ftype = 'OTHER'
                    elif ftype.endswith('archive'):
                        ftype = 'ARCHIVE'
                    self.builder.set_file_type(self.doc, ftype)
                except SPDXValueError:
                    self.value_error('FILE_TYPE', ftype)
        except CardinalityError:
            self.more_than_one_error('file type')

    def p_file_chk_sum(self, f_term, predicate):
        """
        Set file checksum. Assumes SHA1 algorithm without checking.
        """
        try:
            for _s, _p, checksum in self.graph.triples((f_term, predicate, None)):
                for _, _, value in self.graph.triples((checksum, self.spdx_namespace['checksumValue'], None)):
                    self.builder.set_file_chksum(self.doc, six.text_type(value))
        except CardinalityError:
            self.more_than_one_error('File checksum')

    def p_file_lic_conc(self, f_term, predicate):
        """
        Set file licenses concluded.
        """
        try:
            for _, _, licenses in self.graph.triples((f_term, predicate, None)):
                if (licenses, RDF.type, self.spdx_namespace['ConjunctiveLicenseSet']) in self.graph:
                    lics = self.handle_conjunctive_list(licenses)
                    self.builder.set_concluded_license(self.doc, lics)

                elif (licenses, RDF.type, self.spdx_namespace['DisjunctiveLicenseSet']) in self.graph:
                    lics = self.handle_disjunctive_list(licenses)
                    self.builder.set_concluded_license(self.doc, lics)

                else:
                    try:
                        lics = self.handle_lics(licenses)
                        self.builder.set_concluded_license(self.doc, lics)
                    except SPDXValueError:
                        self.value_error('FILE_SINGLE_LICS', licenses)
        except CardinalityError:
            self.more_than_one_error('file {0}'.format(predicate))


class SnippetParser(LicenseParser):
    """
    Helper class for parsing snippet information.
    """

    def __init__(self, builder, logger):
        super(SnippetParser, self).__init__(builder, logger)

    def parse_snippet(self, snippet_term):
        try:
            self.builder.create_snippet(self.doc, snippet_term)
        except SPDXValueError:
            self.value_error('SNIPPET_SPDX_ID_VALUE', snippet_term)

        for _s, _p, o in self.graph.triples((snippet_term, self.spdx_namespace['name'], None)):
            try:
                self.builder.set_snippet_name(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('snippetName')
                break

        for _s, _p, o in self.graph.triples((snippet_term, self.spdx_namespace['licenseComments'], None)):
            try:
                self.builder.set_snippet_lic_comment(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('licenseComments')
                break

        for _s, _p, o in self.graph.triples((snippet_term, RDFS.comment, None)):
            try:
                self.builder.set_snippet_comment(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('comment')
                break

        for _s, _p, o in self.graph.triples((snippet_term, self.spdx_namespace['copyrightText'], None)):
            try:
                self.builder.set_snippet_copyright(self.doc, self.to_special_value(six.text_type(o)))
            except CardinalityError:
                self.more_than_one_error('copyrightText')
                break

        try:
            for _, _, licenses in self.graph.triples(
                    (snippet_term, self.spdx_namespace['licenseConcluded'], None)):
                if (licenses, RDF.type, self.spdx_namespace['ConjunctiveLicenseSet']) in self.graph:
                    lics = self.handle_conjunctive_list(licenses)
                    self.builder.set_snip_concluded_license(self.doc, lics)

                elif (licenses, RDF.type, self.spdx_namespace['DisjunctiveLicenseSet']) in self.graph:
                    lics = self.handle_disjunctive_list(licenses)
                    self.builder.set_snip_concluded_license(self.doc, lics)

                else:
                    try:
                        lics = self.handle_lics(licenses)
                        self.builder.set_snip_concluded_license(self.doc, lics)
                    except SPDXValueError:
                        self.value_error('SNIPPET_SINGLE_LICS', licenses)
        except CardinalityError:
            self.more_than_one_error('package {0}'.format(
                self.spdx_namespace['licenseConcluded']))

        for _, _, info in self.graph.triples(
                (snippet_term, self.spdx_namespace['licenseInfoInSnippet'], None)):
            lic = self.handle_lics(info)
            if lic is not None:
                try:
                    self.builder.set_snippet_lics_info(self.doc, lic)
                except SPDXValueError:
                    self.value_error('SNIPPET_LIC_INFO', lic)

        for _s, _p, o in self.graph.triples(
                (snippet_term, self.spdx_namespace['snippetFromFile'], None)):
            try:
                self.builder.set_snip_from_file_spdxid(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('snippetFromFile')
                break


class ReviewParser(BaseParser):
    """
    Helper class for parsing review information.
    """

    def __init__(self, builder, logger):
        super(ReviewParser, self).__init__(builder, logger)

    def parse_review(self, r_term):
        reviewer = self.get_reviewer(r_term)
        reviewed_date = self.get_review_date(r_term)
        if reviewer is not None:
            self.builder.add_reviewer(self.doc, reviewer)
            if reviewed_date is not None:
                try:
                    self.builder.add_review_date(self.doc, reviewed_date)
                except SPDXValueError:
                    self.value_error('REVIEW_DATE', reviewed_date)
            comment = self.get_review_comment(r_term)
            if comment is not None:
                self.builder.add_review_comment(self.doc, comment)

    def get_review_comment(self, r_term):
        """
        Return review comment or None if found none or more than one.
        Report errors.
        """
        comment_list = list(self.graph.triples((r_term, RDFS.comment, None)))
        if len(comment_list) > 1:
            self.error = True
            msg = 'Review can have at most one comment'
            self.logger.log(msg)
            return
        else:
            return six.text_type(comment_list[0][2])

    def get_review_date(self, r_term):
        """
        Return review date or None if not found.
        Report error on failure.
        Note does not check value format.
        """
        reviewed_list = list(self.graph.triples((r_term, self.spdx_namespace['reviewDate'], None)))
        if len(reviewed_list) != 1:
            self.error = True
            msg = 'Review must have exactlyone review date'
            self.logger.log(msg)
            return
        return six.text_type(reviewed_list[0][2])

    def get_reviewer(self, r_term):
        """
        Return reviewer as creator object or None if failed.
        Report errors on failure.
        """
        reviewer_list = list(self.graph.triples((r_term, self.spdx_namespace['reviewer'], None)))
        if len(reviewer_list) != 1:
            self.error = True
            msg = 'Review must have exactly one reviewer'
            self.logger.log(msg)
            return
        try:
            return self.builder.create_entity(self.doc, six.text_type(reviewer_list[0][2]))
        except SPDXValueError:
            self.value_error('REVIEWER_VALUE', reviewer_list[0][2])


class AnnotationParser(BaseParser):
    """
    Helper class for parsing annotation information.
    """

    def __init__(self, builder, logger):
        super(AnnotationParser, self).__init__(builder, logger)

    def parse_annotation(self, r_term):
        annotator = self.get_annotator(r_term)
        annotation_date = self.get_annotation_date(r_term)
        if annotator is not None:
            self.builder.add_annotator(self.doc, annotator)
            if annotation_date is not None:
                try:
                    self.builder.add_annotation_date(self.doc, annotation_date)
                except SPDXValueError:
                    self.value_error('ANNOTATION_DATE', annotation_date)
            comment = self.get_annotation_comment(r_term)
            if comment is not None:
                self.builder.add_annotation_comment(self.doc, comment)
            annotation_type = self.get_annotation_type(r_term)
            self.builder.add_annotation_type(self.doc, annotation_type)
            try:
                self.builder.set_annotation_spdx_id(self.doc, six.text_type(r_term))
            except CardinalityError:
                self.more_than_one_error('SPDX Identifier Reference')

    def get_annotation_type(self, r_term):
        """
        Return annotation type or None if found none or more than one.
        Report errors on failure.
        """
        for _, _, typ in self.graph.triples((
                r_term, self.spdx_namespace['annotationType'], None)):
            if typ is not None:
                return six.text_type(typ)
            else:
                self.error = True
                msg = 'Annotation must have exactly one annotation type.'
                self.logger.log(msg)
                return

    def get_annotation_comment(self, r_term):
        """
        Return annotation comment or None if found none or more than one.
        Report errors.
        """
        comment_list = list(self.graph.triples((r_term, RDFS.comment, None)))
        if len(comment_list) > 1:
            self.error = True
            msg = 'Annotation can have at most one comment.'
            self.logger.log(msg)
            return
        else:
            return six.text_type(comment_list[0][2])

    def get_annotation_date(self, r_term):
        """
        Return annotation date or None if not found.
        Report error on failure.
        Note does not check value format.
        """
        annotation_date_list = list(self.graph.triples((r_term, self.spdx_namespace['annotationDate'], None)))
        if len(annotation_date_list) != 1:
            self.error = True
            msg = 'Annotation must have exactly one annotation date.'
            self.logger.log(msg)
            return
        return six.text_type(annotation_date_list[0][2])

    def get_annotator(self, r_term):
        """
        Return annotator as creator object or None if failed.
        Report errors on failure.
        """
        annotator_list = list(self.graph.triples((r_term, self.spdx_namespace['annotator'], None)))
        if len(annotator_list) != 1:
            self.error = True
            msg = 'Annotation must have exactly one annotator'
            self.logger.log(msg)
            return
        try:
            return self.builder.create_entity(self.doc, six.text_type(annotator_list[0][2]))
        except SPDXValueError:
            self.value_error('ANNOTATOR_VALUE', annotator_list[0][2])


class Parser(PackageParser, FileParser, SnippetParser, ReviewParser, AnnotationParser):
    """
    RDF/XML file parser.
    """

    def __init__(self, builder, logger):
        super(Parser, self).__init__(builder, logger)

    def parse(self, fil):
        """
        Parse a file and returns a document object.
        fil is a file like object.
        """
        self.error = False
        self.graph = Graph()
        self.graph.parse(file=fil, format='xml')
        self.doc = document.Document()

        for s, _p, o in self.graph.triples((None, RDF.type, self.spdx_namespace['SpdxDocument'])):
            self.parse_doc_fields(s)

        for s, _p, o in self.graph.triples((None, RDF.type, self.spdx_namespace['ExternalDocumentRef'])):
            self.parse_ext_doc_ref(s)

        for s, _p, o in self.graph.triples((None, RDF.type, self.spdx_namespace['CreationInfo'])):
            self.parse_creation_info(s)

        for s, _p, o in self.graph.triples((None, None, self.spdx_namespace['ExtractedLicensingInfo'])):
            self.handle_extracted_license(s)

        for s, _p, o in self.graph.triples((None, RDF.type, self.spdx_namespace['Package'])):
            self.parse_package(s)

        for s, _p, o in self.graph.triples((None, RDF.type, self.spdx_namespace['ExternalRef'])):
            self.parse_pkg_ext_ref(s)

        for s, _p, o in self.graph.triples((None, self.spdx_namespace['referencesFile'], None)):
            self.parse_file(o)

        for s, _p, o in self.graph.triples((None, RDF.type, self.spdx_namespace['Snippet'])):
            self.parse_snippet(s)

        for s, _p, o in self.graph.triples((None, self.spdx_namespace['reviewed'], None)):
            self.parse_review(o)

        for s, _p, o in self.graph.triples((None, self.spdx_namespace['annotation'], None)):
            self.parse_annotation(o)

        validation_messages = []
        # Report extra errors if self.error is False otherwise there will be
        # redundent messages
        validation_messages = self.doc.validate(validation_messages)
        if not self.error:
            if validation_messages:
                for msg in validation_messages:
                    self.logger.log(msg)
                self.error = True
        return self.doc, self.error

    def parse_creation_info(self, ci_term):
        """
        Parse creators, created and comment.
        """
        for _s, _p, o in self.graph.triples((ci_term, self.spdx_namespace['creator'], None)):
            try:
                ent = self.builder.create_entity(self.doc, six.text_type(o))
                self.builder.add_creator(self.doc, ent)
            except SPDXValueError:
                self.value_error('CREATOR_VALUE', o)

        for _s, _p, o in self.graph.triples((ci_term, self.spdx_namespace['created'], None)):
            try:
                self.builder.set_created_date(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('CREATED_VALUE', o)
            except CardinalityError:
                self.more_than_one_error('created')
                break

        for _s, _p, o in self.graph.triples((ci_term, RDFS.comment, None)):
            try:
                self.builder.set_creation_comment(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('CreationInfo comment')
                break
        for _s, _p, o in self.graph.triples((ci_term, self.spdx_namespace['licenseListVersion'], None)):
            try:
                self.builder.set_lics_list_ver(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('licenseListVersion')
                break
            except SPDXValueError:
                self.value_error('LL_VALUE', o)

    def parse_doc_fields(self, doc_term):
        """
        Parse the version, data license, name, SPDX Identifier, namespace,
        and comment.
        """
        try:
            self.builder.set_doc_spdx_id(self.doc, six.text_type(doc_term))
        except SPDXValueError:
            self.value_error('DOC_SPDX_ID_VALUE', doc_term)
        try:
            if doc_term.count('#', 0, len(doc_term)) <= 1:
                doc_namespace = doc_term.split('#')[0]
                self.builder.set_doc_namespace(self.doc, doc_namespace)
            else:
                self.value_error('DOC_NAMESPACE_VALUE', doc_term)
        except SPDXValueError:
            self.value_error('DOC_NAMESPACE_VALUE', doc_term)
        for _s, _p, o in self.graph.triples((doc_term, self.spdx_namespace['specVersion'], None)):
            try:
                self.builder.set_doc_version(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('DOC_VERS_VALUE', o)
            except CardinalityError:
                self.more_than_one_error('specVersion')
                break
        for _s, _p, o in self.graph.triples((doc_term, self.spdx_namespace['dataLicense'], None)):
            try:
                self.builder.set_doc_data_lic(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('DOC_D_LICS', o)
            except CardinalityError:
                self.more_than_one_error('dataLicense')
                break
        for _s, _p, o in self.graph.triples(
                (doc_term, self.spdx_namespace['name'], None)):
            try:
                self.builder.set_doc_name(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('name')
                break
        for _s, _p, o in self.graph.triples((doc_term, RDFS.comment, None)):
            try:
                self.builder.set_doc_comment(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('Document comment')
                break

    def parse_ext_doc_ref(self, ext_doc_ref_term):
        """
        Parse the External Document ID, SPDX Document URI and Checksum.
        """
        for _s, _p, o in self.graph.triples(
                (ext_doc_ref_term,
                 self.spdx_namespace['externalDocumentId'],
                 None)):
            try:
                self.builder.set_ext_doc_id(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('EXT_DOC_REF_VALUE', 'External Document ID')
                break

        for _s, _p, o in self.graph.triples(
                (ext_doc_ref_term,
                 self.spdx_namespace['spdxDocument'],
                 None)):
            try:
                self.builder.set_spdx_doc_uri(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('EXT_DOC_REF_VALUE', 'SPDX Document URI')
                break

        for _s, _p, checksum in self.graph.triples(
                (ext_doc_ref_term, self.spdx_namespace['checksum'], None)):
            for _, _, value in self.graph.triples(
                    (checksum, self.spdx_namespace['checksumValue'], None)):
                try:
                    self.builder.set_chksum(self.doc, six.text_type(value))
                except SPDXValueError:
                    self.value_error('EXT_DOC_REF_VALUE', 'Checksum')
                    break

    def parse_pkg_ext_ref(self, pkg_ext_term):
        """
        Parse the category, type, locator, and comment.
        """
        for _s, _p, o in self.graph.triples((pkg_ext_term,
                                             self.spdx_namespace['referenceCategory'],
                                             None)):
            try:
                self.builder.set_pkg_ext_ref_category(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('PKG_EXT_REF_CATEGORY',
                                 'Package External Reference Category')
                break

        for _s, _p, o in self.graph.triples((pkg_ext_term,
                                             self.spdx_namespace['referenceType'],
                                             None)):
            try:
                self.builder.set_pkg_ext_ref_type(self.doc, six.text_type(o))
            except SPDXValueError:
                self.value_error('PKG_EXT_REF_TYPE',
                                 'Package External Reference Type')
                break

        for _s, _p, o in self.graph.triples((pkg_ext_term,
                                             self.spdx_namespace['referenceLocator'],
                                             None)):
            self.builder.set_pkg_ext_ref_locator(self.doc, six.text_type(o))

        for _s, _p, o in self.graph.triples((pkg_ext_term, RDFS.comment, None)):
            try:
                self.builder.set_pkg_ext_ref_comment(self.doc, six.text_type(o))
            except CardinalityError:
                self.more_than_one_error('Package External Reference Comment')
                break
