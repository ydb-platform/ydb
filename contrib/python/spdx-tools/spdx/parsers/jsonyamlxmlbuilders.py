
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

from spdx.parsers import rdfbuilders
from spdx.parsers import tagvaluebuilders
from spdx.parsers import validations
from spdx.parsers.builderexceptions import SPDXValueError
from spdx.parsers.builderexceptions import CardinalityError
from spdx.parsers.builderexceptions import OrderError


class CreationInfoBuilder(rdfbuilders.CreationInfoBuilder):
    def __init__(self):
        super(CreationInfoBuilder, self).__init__()


class ExternalDocumentRefsBuilder(rdfbuilders.ExternalDocumentRefBuilder):
    def __init__(self):
        super(ExternalDocumentRefsBuilder, self).__init__()


class EntityBuilder(rdfbuilders.EntityBuilder):
    def __init__(self):
        super(EntityBuilder, self).__init__()


class SnippetBuilder(rdfbuilders.SnippetBuilder):
    def __init__(self):
        super(SnippetBuilder, self).__init__()


class ReviewBuilder(rdfbuilders.ReviewBuilder):
    def __init__(self):
        super(ReviewBuilder, self).__init__()


class PackageBuilder(rdfbuilders.PackageBuilder):
    def __init__(self):
        super(PackageBuilder, self).__init__()


class DocBuilder(tagvaluebuilders.DocBuilder):
    def __init__(self):
        super(DocBuilder, self).__init__()

    def set_doc_spdx_id(self, doc, doc_spdx_id_line):
        """
        Set the document SPDX Identifier.
        Raise SPDXValueError if malformed value, CardinalityError
        if already defined.
        """
        if not self.doc_spdx_id_set:
            if doc_spdx_id_line == 'SPDXRef-DOCUMENT' or validations.validate_doc_spdx_id(doc_spdx_id_line):
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
        """
        if not self.doc_comment_set:
            self.doc_comment_set = True
            doc.comment = comment
        else:
            raise CardinalityError('Document::Comment')


class LicenseBuilder(tagvaluebuilders.LicenseBuilder):
    def __init__(self):
        super(LicenseBuilder, self).__init__()

    def set_lic_name(self, doc, name):
        """
        Set license name.
        Raise SPDXValueError if name is not str or utils.NoAssert
        Raise CardinalityError if it is already set
        Raise OrderError if no license id defined.
        """
        if self.has_extr_lic(doc):
            if not self.extr_lic_name_set:
                self.extr_lic_name_set = True
                if validations.validate_extr_lic_name(name, True):
                    self.extr_lic(doc).full_name = name
                    return True
                else:
                    raise SPDXValueError('ExtractedLicense::Name')
            else:
                raise CardinalityError('ExtractedLicense::Name')
        else:
            raise OrderError('ExtractedLicense::Name')

    def set_lic_text(self, doc, text):
        """
        Set license name.
        Raise CardinalityError if it is already set.
        Raise OrderError if no license id defined.
        """
        if self.has_extr_lic(doc):
            if not self.extr_text_set:
                self.extr_text_set = True
                self.extr_lic(doc).text = text
                return True
            else:
                raise CardinalityError('ExtractedLicense::text')
        else:
            raise OrderError('ExtractedLicense::text')

    def set_lic_comment(self, doc, comment):
        """
        Set license comment.
        Raise CardinalityError if it is already set.
        Raise OrderError if no license ID defined.
        """
        if self.has_extr_lic(doc):
            if not self.extr_lic_comment_set:
                self.extr_lic_comment_set = True
                self.extr_lic(doc).comment = comment
                return True
            else:
                raise CardinalityError('ExtractedLicense::comment')
        else:
            raise OrderError('ExtractedLicense::comment')

class FileBuilder(rdfbuilders.FileBuilder):
    def __init__(self):
        super(FileBuilder, self).__init__()

    def set_file_notice(self, doc, text):
        """
        Set file notice
        Raise OrderError if no package or file defined.
        Raise CardinalityError if more than one.
        """
        if self.has_package(doc) and self.has_file(doc):
            if not self.file_notice_set:
                self.file_notice_set = True
                self.file(doc).notice = text
                return True
            else:
                raise CardinalityError('File::Notice')
        else:
            raise OrderError('File::Notice')

    def set_file_type(self, doc, type_value):
        """
        Wrap rdfbuilders.FileBuilder.set_file_type to match the different
        fileType representations.
        """

        type_dict = {
            'fileType_source': 'SOURCE',
            'fileType_binary': 'BINARY',
            'fileType_archive': 'ARCHIVE',
            'fileType_other': 'OTHER'
        }

        return super(FileBuilder, self).set_file_type(doc, type_dict.get(type_value))


class AnnotationBuilder(tagvaluebuilders.AnnotationBuilder):
    def __init__(self):
        super(AnnotationBuilder, self).__init__()

    def add_annotation_comment(self, doc, comment):
        """
        Set the annotation comment.
        Raise CardinalityError if already set.
        Raise OrderError if no annotator defined before.
        """
        if len(doc.annotations) != 0:
            if not self.annotation_comment_set:
                self.annotation_comment_set = True
                doc.annotations[-1].comment = comment
                return True
            else:
                raise CardinalityError('AnnotationComment')
        else:
            raise OrderError('AnnotationComment')


class Builder(DocBuilder, CreationInfoBuilder, ExternalDocumentRefsBuilder, EntityBuilder,
            SnippetBuilder, ReviewBuilder, LicenseBuilder, FileBuilder, PackageBuilder,
            AnnotationBuilder):
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
