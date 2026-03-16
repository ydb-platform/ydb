
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

from rdflib import BNode
from rdflib import Graph
from rdflib import Literal
from rdflib import Namespace
from rdflib import RDF
from rdflib import RDFS
from rdflib import URIRef
from rdflib.compare import to_isomorphic

from spdx import file
from spdx import document
from spdx import config
from spdx import utils
from spdx.writers.tagvalue import InvalidDocumentError


class BaseWriter(object):
    """
    Base class for all Writer classes.
    Provide utility functions and stores shared fields.
    """

    def __init__(self, document, out):
        self.document = document
        self.out = out
        self.doap_namespace = Namespace('http://usefulinc.com/ns/doap#')
        self.spdx_namespace = Namespace("http://spdx.org/rdf/terms#")
        self.graph = Graph()

    def create_checksum_node(self, chksum):
        """
        Return a node representing spdx.checksum.
        """
        chksum_node = BNode()
        type_triple = (chksum_node, RDF.type, self.spdx_namespace.Checksum)
        self.graph.add(type_triple)
        algorithm_triple = (chksum_node, self.spdx_namespace.algorithm, Literal(chksum.identifier))
        self.graph.add(algorithm_triple)
        value_triple = (chksum_node, self.spdx_namespace.checksumValue, Literal(chksum.value))
        self.graph.add(value_triple)
        return chksum_node

    def to_special_value(self, value):
        """
        Return proper spdx term or Literal
        """
        if isinstance(value, utils.NoAssert):
            return self.spdx_namespace.noassertion
        elif isinstance(value, utils.SPDXNone):
            return self.spdx_namespace.none
        else:
            return Literal(value)


class LicenseWriter(BaseWriter):
    """
    Handle all License classes from spdx.document module.
    """

    def __init__(self, document, out):
        super(LicenseWriter, self).__init__(document, out)

    def licenses_from_tree_helper(self, current, licenses):
        if (isinstance(current, (document.LicenseConjunction,
                                 document.LicenseDisjunction))):
            self.licenses_from_tree_helper(current.license_1, licenses)
            self.licenses_from_tree_helper(current.license_2, licenses)
        else:
            licenses.add(self.create_license_helper(current))

    def licenses_from_tree(self, tree):
        """
        Traverse conjunctions and disjunctions like trees and return a
        set of all licenses in it as nodes.
        """
        # FIXME: this is unordered!
        licenses = set()
        self.licenses_from_tree_helper(tree, licenses)
        return licenses

    def create_conjunction_node(self, conjunction):
        """
        Return a node representing a conjunction of licenses.
        """
        node = BNode()
        type_triple = (node, RDF.type, self.spdx_namespace.ConjunctiveLicenseSet)
        self.graph.add(type_triple)
        licenses = self.licenses_from_tree(conjunction)
        for lic in licenses:
            member_triple = (node, self.spdx_namespace.member, lic)
            self.graph.add(member_triple)
        return node

    def create_disjunction_node(self, disjunction):
        """
        Return a node representing a disjunction of licenses.
        """
        node = BNode()
        type_triple = (node, RDF.type, self.spdx_namespace.DisjunctiveLicenseSet)
        self.graph.add(type_triple)
        licenses = self.licenses_from_tree(disjunction)
        for lic in licenses:
            member_triple = (node, self.spdx_namespace.member, lic)
            self.graph.add(member_triple)
        return node

    def create_license_helper(self, lic):
        """
        Handle single(no conjunction/disjunction) licenses.
        Return the created node.
        """
        if isinstance(lic, document.ExtractedLicense):
            return self.create_extracted_license(lic)
        if lic.identifier.rstrip('+') in config.LICENSE_MAP:
            return URIRef(lic.url)
        else:
            matches = [l for l in self.document.extracted_licenses if l.identifier == lic.identifier]
            if len(matches) != 0:
                return self.create_extracted_license(matches[0])
            else:
                raise InvalidDocumentError('Missing extracted license: {0}'.format(lic.identifier))

    def create_extracted_license(self, lic):
        """
        Handle extracted license.
        Return the license node.
        """
        licenses = list(self.graph.triples((None, self.spdx_namespace.licenseId, lic.identifier)))
        if len(licenses) != 0:
            return licenses[0][0]  # return subject in first triple
        else:
            license_node = BNode()
            type_triple = (license_node, RDF.type, self.spdx_namespace.ExtractedLicensingInfo)
            self.graph.add(type_triple)
            ident_triple = (license_node, self.spdx_namespace.licenseId, Literal(lic.identifier))
            self.graph.add(ident_triple)
            text_triple = (license_node, self.spdx_namespace.extractedText, Literal(lic.text))
            self.graph.add(text_triple)
            if lic.full_name is not None:
                name_triple = (license_node, self.spdx_namespace.licenseName, self.to_special_value(lic.full_name))
                self.graph.add(name_triple)
            for ref in lic.cross_ref:
                triple = (license_node, RDFS.seeAlso, URIRef(ref))
                self.graph.add(triple)
            if lic.comment is not None:
                comment_triple = (license_node, RDFS.comment, Literal(lic.comment))
                self.graph.add(comment_triple)
            return license_node

    def create_license_node(self, lic):
        """
        Return a node representing a license.
        Could be a single license (extracted or part of license list.) or
        a conjunction/disjunction of licenses.
        """
        if isinstance(lic, document.LicenseConjunction):
            return self.create_conjunction_node(lic)
        elif isinstance(lic, document.LicenseDisjunction):
            return self.create_disjunction_node(lic)
        else:
            return self.create_license_helper(lic)

    def license_or_special(self, lic):
        """
        Check for special values spdx:none and spdx:noassertion.
        Return the term for the special value or the result of passing
        license to create_license_node.
        """
        if isinstance(lic, utils.NoAssert):
            return self.spdx_namespace.noassertion
        elif isinstance(lic, utils.SPDXNone):
            return self.spdx_namespace.none
        else:
            return self.create_license_node(lic)


class FileWriter(LicenseWriter):
    """
    Write spdx.file.File
    """
    FILE_TYPES = {
        file.FileType.SOURCE: 'fileType_source',
        file.FileType.OTHER: 'fileType_other',
        file.FileType.BINARY: 'fileType_binary',
        file.FileType.ARCHIVE: 'fileType_archive'
    }

    def __init__(self, document, out):
        super(FileWriter, self).__init__(document, out)

    def create_file_node(self, doc_file):
        """
        Create a node for spdx.file.
        """
        file_node = URIRef('http://www.spdx.org/files#{id}'.format(
            id=str(doc_file.spdx_id)))
        type_triple = (file_node, RDF.type, self.spdx_namespace.File)
        self.graph.add(type_triple)

        name_triple = (file_node, self.spdx_namespace.fileName, Literal(doc_file.name))
        self.graph.add(name_triple)

        if doc_file.has_optional_field('comment'):
            comment_triple = (file_node, RDFS.comment, Literal(doc_file.comment))
            self.graph.add(comment_triple)

        if doc_file.has_optional_field('type'):
            ftype = self.spdx_namespace[self.FILE_TYPES[doc_file.type]]
            ftype_triple = (file_node, self.spdx_namespace.fileType, ftype)
            self.graph.add(ftype_triple)

        self.graph.add((file_node, self.spdx_namespace.checksum, self.create_checksum_node(doc_file.chk_sum)))

        conc_lic_node = self.license_or_special(doc_file.conc_lics)
        conc_lic_triple = (file_node, self.spdx_namespace.licenseConcluded, conc_lic_node)
        self.graph.add(conc_lic_triple)

        license_info_nodes = map(self.license_or_special, doc_file.licenses_in_file)
        for lic in license_info_nodes:
            triple = (file_node, self.spdx_namespace.licenseInfoInFile, lic)
            self.graph.add(triple)

        if doc_file.has_optional_field('license_comment'):
            comment_triple = (file_node, self.spdx_namespace.licenseComments, Literal(doc_file.license_comment))
            self.graph.add(comment_triple)

        cr_text_node = self.to_special_value(doc_file.copyright)
        cr_text_triple = (file_node, self.spdx_namespace.copyrightText, cr_text_node)
        self.graph.add(cr_text_triple)

        if doc_file.has_optional_field('notice'):
            notice_triple = (file_node, self.spdx_namespace.noticeText, doc_file.notice)
            self.graph.add(notice_triple)

        contrib_nodes = map(lambda c: Literal(c), doc_file.contributors)
        contrib_triples = [(file_node, self.spdx_namespace.fileContributor, node) for node in contrib_nodes]
        for triple in contrib_triples:
            self.graph.add(triple)

        return file_node

    def files(self):
        """
        Return list of file nodes.
        """
        return map(self.create_file_node, self.document.files)

    def add_file_dependencies_helper(self, doc_file):
        """
        Handle dependencies for a single file.
        - doc_file - instance of spdx.file.File.
        """
        subj_triples = list(self.graph.triples((None, self.spdx_namespace.fileName, Literal(doc_file.name))))
        if len(subj_triples) != 1:
            raise InvalidDocumentError('Could not find dependency subject {0}'.format(doc_file.name))
        subject_node = subj_triples[0][0]
        for dependency in doc_file.dependencies:
            dep_triples = list(self.graph.triples((None, self.spdx_namespace.fileName, Literal(dependency))))
            if len(dep_triples) == 1:
                dep_node = dep_triples[0][0]
                dep_triple = (subject_node, self.spdx_namespace.fileDependency, dep_node)
                self.graph.add(dep_triple)
            else:
                print('Warning could not resolve file dependency {0} -> {1}'.format(doc_file.name, dependency))

    def add_file_dependencies(self):
        """
        Add file dependencies to the graph.
        Called after all files have been added.
        """
        for doc_file in self.document.files:
            self.add_file_dependencies_helper(doc_file)


class SnippetWriter(LicenseWriter):

    """
    Write spdx.snippet.Snippet
    """

    def __init__(self, document, out):
        super(SnippetWriter, self).__init__(document, out)

    def create_snippet_node(self, snippet):
        """
        Return a snippet node.
        """
        snippet_node = URIRef('http://spdx.org/rdf/terms/Snippet#' + snippet.spdx_id)
        type_triple = (snippet_node, RDF.type, self.spdx_namespace.Snippet)
        self.graph.add(type_triple)

        if snippet.has_optional_field('comment'):
            comment_triple = (snippet_node, RDFS.comment, Literal(snippet.comment))
            self.graph.add(comment_triple)

        if snippet.has_optional_field('name'):
            name_triple = (snippet_node, self.spdx_namespace.name, Literal(snippet.name))
            self.graph.add(name_triple)

        if snippet.has_optional_field('license_comment'):
            lic_comment_triple = (snippet_node, self.spdx_namespace.licenseComments,
                                  Literal(snippet.license_comment))
            self.graph.add(lic_comment_triple)

        cr_text_node = self.to_special_value(snippet.copyright)
        cr_text_triple = (snippet_node, self.spdx_namespace.copyrightText, cr_text_node)
        self.graph.add(cr_text_triple)

        snip_from_file_triple = (snippet_node, self.spdx_namespace.snippetFromFile,
                                 Literal(snippet.snip_from_file_spdxid))
        self.graph.add(snip_from_file_triple)

        conc_lic_node = self.license_or_special(snippet.conc_lics)
        conc_lic_triple = (
            snippet_node, self.spdx_namespace.licenseConcluded, conc_lic_node)
        self.graph.add(conc_lic_triple)

        license_info_nodes = map(self.license_or_special,
                                 snippet.licenses_in_snippet)
        for lic in license_info_nodes:
            triple = (
            snippet_node, self.spdx_namespace.licenseInfoInSnippet, lic)
            self.graph.add(triple)

        return snippet_node

    def snippets(self):
        """
        Return list of snippet nodes.
        """
        return map(self.create_snippet_node, self.document.snippet)


class ReviewInfoWriter(BaseWriter):

    """
    Write spdx.review.Review
    """

    def __init__(self, document, out):
        super(ReviewInfoWriter, self).__init__(document, out)

    def create_review_node(self, review):
        """
        Return a review node.
        """
        review_node = BNode()
        type_triple = (review_node, RDF.type, self.spdx_namespace.Review)
        self.graph.add(type_triple)

        reviewer_node = Literal(review.reviewer.to_value())
        self.graph.add((review_node, self.spdx_namespace.reviewer, reviewer_node))
        reviewed_date_node = Literal(review.review_date_iso_format)
        reviewed_triple = (review_node, self.spdx_namespace.reviewDate, reviewed_date_node)
        self.graph.add(reviewed_triple)
        if review.has_comment:
            comment_node = Literal(review.comment)
            comment_triple = (review_node, RDFS.comment, comment_node)
            self.graph.add(comment_triple)

        return review_node

    def reviews(self):
        "Returns a list of review nodes"
        return map(self.create_review_node, self.document.reviews)


class AnnotationInfoWriter(BaseWriter):
    """
    Write spdx.annotation.Annotation
    """

    def __init__(self, document, out):
        super(AnnotationInfoWriter, self).__init__(document, out)

    def create_annotation_node(self, annotation):
        """
        Return an annotation node.
        """
        annotation_node = URIRef(str(annotation.spdx_id))
        type_triple = (annotation_node, RDF.type, self.spdx_namespace.Annotation)
        self.graph.add(type_triple)

        annotator_node = Literal(annotation.annotator.to_value())
        self.graph.add((annotation_node, self.spdx_namespace.annotator, annotator_node))
        annotation_date_node = Literal(annotation.annotation_date_iso_format)
        annotation_triple = (annotation_node, self.spdx_namespace.annotationDate, annotation_date_node)
        self.graph.add(annotation_triple)
        if annotation.has_comment:
            comment_node = Literal(annotation.comment)
            comment_triple = (annotation_node, RDFS.comment, comment_node)
            self.graph.add(comment_triple)
        annotation_type_node = Literal(annotation.annotation_type)
        annotation_type_triple = (annotation_node, self.spdx_namespace.annotationType, annotation_type_node)
        self.graph.add(annotation_type_triple)

        return annotation_node

    def annotations(self):
        """
        Return a list of annotation nodes
        """
        return map(self.create_annotation_node, self.document.annotations)


class CreationInfoWriter(BaseWriter):

    """
    Write class spdx.creationinfo.CreationInfo
    """

    def __init__(self, document, out):
        super(CreationInfoWriter, self).__init__(document, out)

    def creators(self):
        """
        Return a list of creator nodes.
        Note: Does not add anything to the graph.
        """
        return map(lambda c: Literal(c.to_value()), self.document.creation_info.creators)

    def create_creation_info(self):
        """
        Add and return a creation info node to graph
        """
        ci_node = BNode()
        # Type property
        type_triple = (ci_node, RDF.type, self.spdx_namespace.CreationInfo)
        self.graph.add(type_triple)

        created_date = Literal(self.document.creation_info.created_iso_format)
        created_triple = (ci_node, self.spdx_namespace.created, created_date)
        self.graph.add(created_triple)

        creators = self.creators()
        for creator in creators:
            self.graph.add((ci_node, self.spdx_namespace.creator, creator))

        if self.document.creation_info.has_comment:
            comment_node = Literal(self.document.creation_info.comment)
            comment_triple = (ci_node, RDFS.comment, comment_node)
            self.graph.add(comment_triple)

        return ci_node


class ExternalDocumentRefWriter(BaseWriter):
    """
    Write class spdx.external_document_ref.ExternalDocumentRef
    """

    def __init__(self, document, out):
        super(ExternalDocumentRefWriter, self).__init__(document, out)

    def create_external_document_ref_node(self, ext_document_references):
        """
        Add and return a creation info node to graph
        """
        ext_doc_ref_node = BNode()
        type_triple = (ext_doc_ref_node, RDF.type, self.spdx_namespace.ExternalDocumentRef)
        self.graph.add(type_triple)

        ext_doc_id = Literal(
            ext_document_references.external_document_id)
        ext_doc_id_triple = (
            ext_doc_ref_node, self.spdx_namespace.externalDocumentId, ext_doc_id)
        self.graph.add(ext_doc_id_triple)

        doc_uri = Literal(
            ext_document_references.spdx_document_uri)
        doc_uri_triple = (
            ext_doc_ref_node, self.spdx_namespace.spdxDocument, doc_uri)
        self.graph.add(doc_uri_triple)

        checksum_node = self.create_checksum_node(
            ext_document_references.check_sum)
        self.graph.add(
            (ext_doc_ref_node, self.spdx_namespace.checksum, checksum_node))

        return ext_doc_ref_node

    def ext_doc_refs(self):
        """
        Return a list of review nodes
        """
        return map(self.create_external_document_ref_node,
                   self.document.ext_document_references)


class PackageWriter(LicenseWriter):

    """
    Write spdx.package.Package
    """

    def __init__(self, document, out):
        super(PackageWriter, self).__init__(document, out)

    def package_verif_node(self, package):
        """
        Return a node representing package verification code.
        """
        verif_node = BNode()
        type_triple = (verif_node, RDF.type, self.spdx_namespace.PackageVerificationCode)
        self.graph.add(type_triple)
        value_triple = (verif_node, self.spdx_namespace.packageVerificationCodeValue, Literal(package.verif_code))
        self.graph.add(value_triple)
        excl_file_nodes = map(
            lambda excl: Literal(excl), package.verif_exc_files)
        excl_predicate = self.spdx_namespace.packageVerificationCodeExcludedFile
        excl_file_triples = [(verif_node, excl_predicate, xcl_file) for xcl_file in excl_file_nodes]
        for trp in excl_file_triples:
            self.graph.add(trp)
        return verif_node

    def handle_package_literal_optional(self, package, package_node, predicate, field):
        """
        Check if optional field is set.
        If so it adds the triple (package_node, predicate, $) to the graph.
        Where $ is a literal or special value term of the value of the field.
        """
        if package.has_optional_field(field):
            value = getattr(package, field, None)
            value_node = self.to_special_value(value)
            triple = (package_node, predicate, value_node)
            self.graph.add(triple)

    def handle_pkg_optional_fields(self, package, package_node):
        """
        Write package optional fields.
        """
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.versionInfo, 'version')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.packageFileName, 'file_name')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.supplier, 'supplier')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.originator, 'originator')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.sourceInfo, 'source_info')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.licenseComments, 'license_comment')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.summary, 'summary')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.description, 'description')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.comment, 'comment')
        self.handle_package_literal_optional(package, package_node, self.spdx_namespace.filesAnalyzed, 'files_analyzed')


        if package.has_optional_field('check_sum'):
            checksum_node = self.create_checksum_node(package.check_sum)
            self.graph.add((package_node, self.spdx_namespace.checksum, checksum_node))

        if package.has_optional_field('homepage'):
            homepage_node = URIRef(self.to_special_value(package.homepage))
            homepage_triple = (package_node, self.doap_namespace.homepage, homepage_node)
            self.graph.add(homepage_triple)

    def create_package_node(self, package):
        """
        Return a Node representing the package.
        Files must have been added to the graph before this method is called.
        """
        package_node = URIRef('http://www.spdx.org/tools#SPDXRef-Package')
        type_triple = (package_node, RDF.type, self.spdx_namespace.Package)
        self.graph.add(type_triple)
        # Package SPDXID
        if package.spdx_id:
            pkg_spdx_id = URIRef(package.spdx_id)
            pkg_spdx_id_triple = (package_node, self.spdx_namespace.Package,
                                  pkg_spdx_id)
            self.graph.add(pkg_spdx_id_triple)
        # Handle optional fields:
        self.handle_pkg_optional_fields(package, package_node)
        # package name
        name_triple = (package_node, self.spdx_namespace.name, Literal(package.name))
        self.graph.add(name_triple)
        # Package download location
        down_loc_node = (package_node, self.spdx_namespace.downloadLocation, self.to_special_value(package.download_location))
        self.graph.add(down_loc_node)
        # Handle package verification
        verif_node = self.package_verif_node(package)
        verif_triple = (package_node, self.spdx_namespace.packageVerificationCode, verif_node)
        self.graph.add(verif_triple)
        # Handle concluded license
        conc_lic_node = self.license_or_special(package.conc_lics)
        conc_lic_triple = (package_node, self.spdx_namespace.licenseConcluded, conc_lic_node)
        self.graph.add(conc_lic_triple)
        # Handle declared license
        decl_lic_node = self.license_or_special(package.license_declared)
        decl_lic_triple = (package_node, self.spdx_namespace.licenseDeclared, decl_lic_node)
        self.graph.add(decl_lic_triple)
        # Package licenses from files
        licenses_from_files_nodes = map(lambda el: self.license_or_special(el), package.licenses_from_files)
        lic_from_files_predicate = self.spdx_namespace.licenseInfoFromFiles
        lic_from_files_triples = [(package_node, lic_from_files_predicate, node) for node in licenses_from_files_nodes]
        for triple in lic_from_files_triples:
            self.graph.add(triple)
        # Copyright Text
        cr_text_node = self.to_special_value(package.cr_text)
        cr_text_triple = (package_node, self.spdx_namespace.copyrightText, cr_text_node)
        self.graph.add(cr_text_triple)
        # Handle files
        self.handle_package_has_file(package, package_node)
        return package_node

    def packages(self):
        """
        Return a node that represents the package in the graph.
        Call this function to write package info.
        """
        # TODO: In the future this may be a list to support SPDX 2.0
        return self.create_package_node(self.document.package)

    def handle_package_has_file_helper(self, pkg_file):
        """
        Return node representing pkg_file
        pkg_file should be instance of spdx.file.
        """
        nodes = list(self.graph.triples((None, self.spdx_namespace.fileName, Literal(pkg_file.name))))
        if len(nodes) == 1:
            return nodes[0][0]
        else:
            raise InvalidDocumentError('handle_package_has_file_helper could not' +
                                       ' find file node for file: {0}'.format(pkg_file.name))

    def handle_package_has_file(self, package, package_node):
        """
        Add hasFile triples to graph.
        Must be called after files have been added.
        """
        file_nodes = map(self.handle_package_has_file_helper, package.files)
        triples = [(package_node, self.spdx_namespace.hasFile, node) for node in file_nodes]
        for triple in triples:
            self.graph.add(triple)


class PackageExternalRefWriter(BaseWriter):
    """
    Write class spdx.package.ExternalPackageRef
    """

    def __init__(self, document, out):
        super(PackageExternalRefWriter, self).__init__(document, out)

    def create_package_external_ref_node(self, pkg_ext_refs):
        """
        Add and return an external package reference node to graph.
        """
        pkg_ext_ref_node = BNode()
        pkg_ext_ref_triple = (pkg_ext_ref_node, RDF.type, self.spdx_namespace.ExternalRef)
        self.graph.add(pkg_ext_ref_triple)

        pkg_ext_ref_category = Literal(pkg_ext_refs.category)
        pkg_ext_ref_category_triple = (
            pkg_ext_ref_node, self.spdx_namespace.referenceCategory, pkg_ext_ref_category)
        self.graph.add(pkg_ext_ref_category_triple)

        pkg_ext_ref_type = Literal(pkg_ext_refs.pkg_ext_ref_type)
        pkg_ext_ref_type_triple = (
            pkg_ext_ref_node, self.spdx_namespace.referenceType, pkg_ext_ref_type)
        self.graph.add(pkg_ext_ref_type_triple)

        pkg_ext_ref_locator = Literal(pkg_ext_refs.locator)
        pkg_ext_ref_locator_triple = (
            pkg_ext_ref_node, self.spdx_namespace.referenceLocator, pkg_ext_ref_locator)
        self.graph.add(pkg_ext_ref_locator_triple)

        pkg_ext_ref_comment = Literal(pkg_ext_refs.comment)
        pkg_ext_ref_comment_triple = (
            pkg_ext_ref_node, RDFS.comment, pkg_ext_ref_comment)
        self.graph.add(pkg_ext_ref_comment_triple)

        return pkg_ext_ref_node

    def pkg_ext_refs(self):
        """
        Return a list of package external references.
        """
        return map(self.create_package_external_ref_node,
                   self.document.package.pkg_ext_refs)


class Writer(CreationInfoWriter, ReviewInfoWriter, FileWriter, PackageWriter,
            PackageExternalRefWriter, ExternalDocumentRefWriter, AnnotationInfoWriter,
            SnippetWriter):
    """
    Warpper for other writers to write all fields of spdx.document.Document
    Call `write()` to start writing.
    """

    def __init__(self, document, out):
        """
        - document is spdx.document instance that will be written.
        - out is a file-like object that will be written to.
        """
        super(Writer, self).__init__(document, out)

    def create_doc(self):
        """
        Add and return the root document node to graph.
        """
        doc_node = URIRef('http://www.spdx.org/tools#SPDXRef-DOCUMENT')
        # Doc type
        self.graph.add((doc_node, RDF.type, self.spdx_namespace.SpdxDocument))
        # Version
        vers_literal = Literal(str(self.document.version))
        self.graph.add((doc_node, self.spdx_namespace.specVersion, vers_literal))
        # Data license
        data_lics = URIRef(self.document.data_license.url)
        self.graph.add((doc_node, self.spdx_namespace.dataLicense, data_lics))
        if self.document.name:
            doc_name = URIRef(self.document.name)
            self.graph.add((doc_node, self.spdx_namespace.name, doc_name))
        return doc_node

    def write(self):
        doc_node = self.create_doc()
        # Add creation info
        creation_info_node = self.create_creation_info()
        ci_triple = (doc_node, self.spdx_namespace.creationInfo, creation_info_node)
        self.graph.add(ci_triple)
        # Add review info
        review_nodes = self.reviews()
        for review in review_nodes:
            self.graph.add((doc_node, self.spdx_namespace.reviewed, review))
        # Add external document references info
        ext_doc_ref_nodes = self.ext_doc_refs()
        for ext_doc_ref in ext_doc_ref_nodes:
            ext_doc_ref_triple = (doc_node,
                                  self.spdx_namespace.externalDocumentRef,
                                  ext_doc_ref)
            self.graph.add(ext_doc_ref_triple)
        # Add extracted licenses
        licenses = map(
            self.create_extracted_license, self.document.extracted_licenses)
        for lic in licenses:
            self.graph.add((doc_node, self.spdx_namespace.hasExtractedLicensingInfo, lic))
        # Add files
        files = self.files()
        for file_node in files:
            self.graph.add((doc_node, self.spdx_namespace.referencesFile, file_node))
        self.add_file_dependencies()
        # Add package
        package_node = self.packages()
        package_triple = (doc_node, self.spdx_namespace.describesPackage, package_node)
        self.graph.add(package_triple)
        # Add snippet
        snippet_nodes = self.snippets()
        for snippet in snippet_nodes:
            self.graph.add((doc_node, self.spdx_namespace.Snippet, snippet))

        # normalize the graph to ensure that the sort order is stable
        self.graph = to_isomorphic(self.graph)

        # Write file
        self.graph.serialize(self.out, 'pretty-xml', encoding='utf-8')


def write_document(document, out, validate=True):
    """
    Write an SPDX RDF document.
    - document - spdx.document instance.
    - out - file like object that will be written to.
    Optionally `validate` the document before writing and raise
    InvalidDocumentError if document.validate returns False.
    """

    if validate:
        messages = []
        messages = document.validate(messages)
        if messages:
            raise InvalidDocumentError(messages)

    writer = Writer(document, out)
    writer.write()
