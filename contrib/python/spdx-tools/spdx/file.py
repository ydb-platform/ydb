
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
import hashlib

import six

from spdx import checksum
from spdx import document
from spdx import utils


class FileType(object):
    SOURCE = 1
    BINARY = 2
    ARCHIVE = 3
    OTHER = 4

@total_ordering
class File(object):
    """
    Represent an SPDX file.
    Fields:
    - name: File name, str mandatory one.
    - spdx_id: Uniquely identify any element in an SPDX document which may be
    referenced by other elements. Mandatory, one. Type: str.
    - comment: File comment str, Optional zero or one.
    - type: one of FileType.SOURCE, FileType.BINARY, FileType.ARCHIVE
      and FileType.OTHER, optional zero or one.
    - chk_sum: SHA1, Mandatory one.
    - conc_lics: Mandatory one. document.License or utils.NoAssert or utils.SPDXNone.
    - licenses_in_file: list of licenses found in file, mandatory one or more.
      document.License or utils.SPDXNone or utils.NoAssert.
    - document.license or utils.NoAssert or utils.SPDXNone.
    - license_comment: Optional.
    - copyright: Copyright text, Mandatory one. utils.NoAssert or utils.SPDXNone or str.
    - notice: optional One, str.
    - contributors: List of strings.
    - dependencies: list of file locations.
    - artifact_of_project_name: list of project names, possibly empty.
    - artifact_of_project_home: list of project home page, possibly empty.
    - artifact_of_project_uri: list of project uris, possibly empty.
    """

    def __init__(self, name, spdx_id=None, chk_sum=None):
        self.name = name
        self.spdx_id = spdx_id
        self.comment = None
        self.type = None
        self.chk_sum = chk_sum
        self.conc_lics = None
        self.licenses_in_file = []
        self.license_comment = None
        self.copyright = None
        self.notice = None
        self.contributors = []
        self.dependencies = []
        self.artifact_of_project_name = []
        self.artifact_of_project_home = []
        self.artifact_of_project_uri = []

    def __eq__(self, other):
        return isinstance(other, File) and self.name == other.name

    def __lt__(self, other):
        return self.name < other.name

    def add_lics(self, lics):
        self.licenses_in_file.append(lics)

    def add_contrib(self, contrib):
        self.contributors.append(contrib)

    def add_depend(self, depend):
        self.dependencies.append(depend)

    def add_artifact(self, symbol, value):
        """
        Add value as artifact_of_project{symbol}.
        """
        symbol = 'artifact_of_project_{}'.format(symbol)
        artifact = getattr(self, symbol)
        artifact.append(value)

    def validate(self, messages):
        """Validates the fields and appends user friendly messages
        to messages parameter if there are errors.
        """
        messages = self.validate_concluded_license(messages)
        messages = self.validate_type(messages)
        messages = self.validate_checksum(messages)
        messages = self.validate_licenses_in_file(messages)
        messages = self.validate_copyright(messages)
        messages = self.validate_artifacts(messages)
        messages = self.validate_spdx_id(messages)

        return messages

    def validate_spdx_id(self, messages):
        if self.spdx_id is None:
            messages = messages + ['File has no SPDX Identifier.']

        return messages

    def validate_copyright(self, messages):
        if not isinstance(
            self.copyright,
            (six.string_types, six.text_type, utils.NoAssert, utils.SPDXNone)
        ):
            messages = messages + [
                'File copyright must be str or unicode or '
                'utils.NoAssert or utils.SPDXNone'
            ]

        return messages

    def validate_artifacts(self, messages):
        if (len(self.artifact_of_project_home) <
            max(len(self.artifact_of_project_uri),
                len(self.artifact_of_project_name))):
            messages = messages + [
                'File must have as much artifact of project as uri or homepage']

        return messages

    def validate_licenses_in_file(self, messages):
        # FIXME: what are we testing the length of a list? or?
        if len(self.licenses_in_file) == 0:
            messages = messages + [
                'File must have at least one license in file.'
            ]

        return messages

    def validate_concluded_license(self, messages):
        # FIXME: use isinstance instead??
        if not isinstance(self.conc_lics, (document.License, utils.NoAssert,
                                           utils.SPDXNone)):
            messages = messages + [
                'File concluded license must be one of '
                'document.License, utils.NoAssert or utils.SPDXNone'
            ]

        return messages

    def validate_type(self, messages):
        if self.type not in [
            None, FileType.SOURCE, FileType.OTHER, FileType.BINARY,
            FileType.ARCHIVE
        ]:
            messages = messages + [
                'File type must be one of the constants defined in '
                'class spdx.file.FileType'
            ]

        return messages

    def validate_checksum(self, messages):
        if not isinstance(self.chk_sum, checksum.Algorithm):
            messages = messages + [
                'File checksum must be instance of spdx.checksum.Algorithm'
            ]
        else:
            if not self.chk_sum.identifier == 'SHA1':
                messages = messages + ['File checksum algorithm must be SHA1']

        return messages

    def calc_chksum(self):
        BUFFER_SIZE = 65536

        file_sha1 = hashlib.sha1()
        with open(self.name, 'rb') as file_handle:
            while True:
                data = file_handle.read(BUFFER_SIZE)
                if not data:
                    break
                file_sha1.update(data)

        return file_sha1.hexdigest()

    def has_optional_field(self, field):
        return getattr(self, field, None) is not None
