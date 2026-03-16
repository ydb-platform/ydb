
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

from ply import lex


class Lexer(object):
    reserved = {
        # Top level fields
        'SPDXVersion': 'DOC_VERSION',
        'DataLicense': 'DOC_LICENSE',
        'DocumentName': 'DOC_NAME',
        'SPDXID': 'SPDX_ID',
        'DocumentComment': 'DOC_COMMENT',
        'DocumentNamespace': 'DOC_NAMESPACE',
        'ExternalDocumentRef': 'EXT_DOC_REF',
        # Creation info
        'Creator': 'CREATOR',
        'Created': 'CREATED',
        'CreatorComment': 'CREATOR_COMMENT',
        'LicenseListVersion': 'LIC_LIST_VER',
        # Review info
        'Reviewer': 'REVIEWER',
        'ReviewDate': 'REVIEW_DATE',
        'ReviewComment': 'REVIEW_COMMENT',
        # Annotation info
        'Annotator': 'ANNOTATOR',
        'AnnotationDate': 'ANNOTATION_DATE',
        'AnnotationComment': 'ANNOTATION_COMMENT',
        'AnnotationType': 'ANNOTATION_TYPE',
        'SPDXREF': 'ANNOTATION_SPDX_ID',
        # Package Fields
        'PackageName': 'PKG_NAME',
        'PackageVersion': 'PKG_VERSION',
        'PackageDownloadLocation': 'PKG_DOWN',
        'FilesAnalyzed': 'PKG_FILES_ANALYZED',
        'PackageSummary': 'PKG_SUM',
        'PackageSourceInfo': 'PKG_SRC_INFO',
        'PackageFileName': 'PKG_FILE_NAME',
        'PackageSupplier': 'PKG_SUPPL',
        'PackageOriginator': 'PKG_ORIG',
        'PackageChecksum': 'PKG_CHKSUM',
        'PackageVerificationCode': 'PKG_VERF_CODE',
        'PackageDescription': 'PKG_DESC',
        'PackageComment': 'PKG_COMMENT',
        'PackageLicenseDeclared': 'PKG_LICS_DECL',
        'PackageLicenseConcluded': 'PKG_LICS_CONC',
        'PackageLicenseInfoFromFiles': 'PKG_LICS_FFILE',
        'PackageLicenseComments': 'PKG_LICS_COMMENT',
        'PackageCopyrightText': 'PKG_CPY_TEXT',
        'PackageHomePage': 'PKG_HOME',
        'ExternalRef': 'PKG_EXT_REF',
        'ExternalRefComment': 'PKG_EXT_REF_COMMENT',
        # Files
        'FileName': 'FILE_NAME',
        'FileType': 'FILE_TYPE',
        'FileChecksum': 'FILE_CHKSUM',
        'LicenseConcluded': 'FILE_LICS_CONC',
        'LicenseInfoInFile': 'FILE_LICS_INFO',
        'FileCopyrightText': 'FILE_CR_TEXT',
        'LicenseComments': 'FILE_LICS_COMMENT',
        'FileComment': 'FILE_COMMENT',
        'FileNotice': 'FILE_NOTICE',
        'FileContributor': 'FILE_CONTRIB',
        'FileDependency': 'FILE_DEP',
        'ArtifactOfProjectName': 'ART_PRJ_NAME',
        'ArtifactOfProjectHomePage': 'ART_PRJ_HOME',
        'ArtifactOfProjectURI': 'ART_PRJ_URI',
        # License
        'LicenseID': 'LICS_ID',
        'ExtractedText': 'LICS_TEXT',
        'LicenseName': 'LICS_NAME',
        'LicenseCrossReference': 'LICS_CRS_REF',
        'LicenseComment': 'LICS_COMMENT',
        # Snippet
        'SnippetSPDXID': 'SNIPPET_SPDX_ID',
        'SnippetName': 'SNIPPET_NAME',
        'SnippetComment': 'SNIPPET_COMMENT',
        'SnippetCopyrightText': 'SNIPPET_CR_TEXT',
        'SnippetLicenseComments': 'SNIPPET_LICS_COMMENT',
        'SnippetFromFileSPDXID': 'SNIPPET_FILE_SPDXID',
        'SnippetLicenseConcluded': 'SNIPPET_LICS_CONC',
        'LicenseInfoInSnippet': 'SNIPPET_LICS_INFO',
        # Common
        'NOASSERTION': 'NO_ASSERT',
        'UNKNOWN': 'UN_KNOWN',
        'NONE': 'NONE',
        'SOURCE': 'SOURCE',
        'BINARY': 'BINARY',
        'ARCHIVE': 'ARCHIVE',
        'OTHER': 'OTHER'
    }
    states = (('text', 'exclusive'),)

    tokens = ['TEXT', 'TOOL_VALUE', 'UNKNOWN_TAG',
              'ORG_VALUE', 'PERSON_VALUE',
              'DATE', 'LINE', 'CHKSUM', 'DOC_REF_ID',
              'DOC_URI', 'EXT_DOC_REF_CHKSUM'] + list(reserved.values())

    def t_text(self, t):
        r':\s*<text>'
        t.lexer.text_start = t.lexer.lexpos - len('<text>')
        t.lexer.begin('text')

    def t_text_end(self, t):
        r'</text>\s*'
        t.type = 'TEXT'
        t.value = t.lexer.lexdata[
            t.lexer.text_start:t.lexer.lexpos]
        t.lexer.lineno += t.value.count('\n')
        t.value = t.value.strip()
        t.lexer.begin('INITIAL')
        return t

    def t_text_any(self, t):
        r'.|\n'
        pass

    def t_text_error(self, t):
        print('Lexer error in text state')

    def t_CHKSUM(self, t):
        r':\s*SHA1:\s*[a-f0-9]{40,40}'
        t.value = t.value[1:].strip()
        return t

    def t_DOC_REF_ID(self, t):
        r':\s*DocumentRef-([A-Za-z0-9\+\.\-]+)'
        t.value = t.value[1:].strip()
        return t

    def t_DOC_URI(self, t):
        r'\s*((ht|f)tps?:\/\/\S*)'
        t.value = t.value.strip()
        return t

    def t_EXT_DOC_REF_CHKSUM(self, t):
        r'\s*SHA1:\s*[a-f0-9]{40,40}'
        t.value = t.value[1:].strip()
        return t

    def t_TOOL_VALUE(self, t):
        r':\s*Tool:.+'
        t.value = t.value[1:].strip()
        return t

    def t_ORG_VALUE(self, t):
        r':\s*Organization:.+'
        t.value = t.value[1:].strip()
        return t

    def t_PERSON_VALUE(self, t):
        r':\s*Person:.+'
        t.value = t.value[1:].strip()
        return t

    def t_DATE(self, t):
        r':\s*\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ'
        t.value = t.value[1:].strip()
        return t

    def t_KEYWORD_AS_TAG(self, t):
        r'[a-zA-Z]+'
        t.type = self.reserved.get(t.value, 'UNKNOWN_TAG')
        t.value = t.value.strip()
        return t

    def t_LINE_OR_KEYWORD_VALUE(self, t):
        r':.+'
        t.value = t.value[1:].strip()
        if t.value in self.reserved.keys():
            t.type = self.reserved[t.value]
        else:
            t.type = 'LINE'
        return t

    def t_comment(self, t):
        r'\#.*'
        pass

    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    def t_whitespace(self, t):
        r'\s+'
        pass

    def build(self, **kwargs):
        self.lexer = lex.lex(module=self, **kwargs)

    def token(self):
        return self.lexer.token()

    def input(self, data):
        self.lexer.input(data)

    def t_error(self, t):
        t.lexer.skip(1)
        t.value = 'Lexer error'
        return t
