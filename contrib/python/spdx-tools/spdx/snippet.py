# Copyright (c) 2018 Yash M. Nisar
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
from spdx import utils


class Snippet(object):
    """
    Represents an analyzed snippet.
    Fields:
     - spdx_id: Uniquely identify any element in an SPDX document which may be
     referenced  by other elements. Mandatory, one per snippet if the snippet
     is present.
     - name: Name of the snippet. Optional, one. Type: str.
     - comment: General comments about the snippet. Optional, one. Type: str.
     - copyright: Copyright text. Mandatory, one. Type: str.
     - license_comment: Relevant background references or analysis that went
     in to arriving at the Concluded License for a snippet. Optional, one.
     - snip_from_file_spdxid:  Uniquely identify the file in an SPDX document
     which this snippet is associated with. Mandatory, one. Type: str.
     Type: str.
     - conc_lics: Contains the license the SPDX file creator has concluded as
     governing the snippet or alternative values if the governing license
     cannot be determined. Mandatory one. Type: document.License or
     utils.NoAssert or utils.SPDXNone.
     - licenses_in_snippet: The list of licenses found in the snippet.
     Mandatory, one or more. Type: document.License or utils.SPDXNone or
     utils.NoAssert.
    """

    def __init__(self, spdx_id=None, copyright=None,
                 snip_from_file_spdxid=None, conc_lics=None):
        self.spdx_id = spdx_id
        self.name = None
        self.comment = None
        self.copyright = copyright
        self.license_comment = None
        self.snip_from_file_spdxid = snip_from_file_spdxid
        self.conc_lics = conc_lics
        self.licenses_in_snippet = []

    def add_lics(self, lics):
        self.licenses_in_snippet.append(lics)

    def validate(self, messages=None):
        """
        Validate fields of the snippet and update the messages list with user
        friendly error messages for display.
        """
        messages = self.validate_spdx_id(messages)
        messages = self.validate_copyright_text(messages)
        messages = self.validate_snip_from_file_spdxid(messages)
        messages = self.validate_concluded_license(messages)
        messages = self.validate_licenses_in_snippet(messages)

        return messages

    def validate_spdx_id(self, messages=None):
        if self.spdx_id is None:
            messages = messages + ['Snippet has no SPDX Identifier.']

        return messages

    def validate_copyright_text(self, messages=None):
        if not isinstance(
            self.copyright,
                (six.string_types, six.text_type, utils.NoAssert,
                 utils.SPDXNone)):
            messages = messages + [
                'Snippet copyright must be str or unicode or utils.NoAssert or utils.SPDXNone'
            ]

        return messages

    def validate_snip_from_file_spdxid(self, messages=None):
        if self.snip_from_file_spdxid is None:
            messages = messages + ['Snippet has no Snippet from File SPDX Identifier.']

        return messages

    def validate_concluded_license(self, messages=None):
        if not isinstance(self.conc_lics, (document.License, utils.NoAssert,
                                       utils.SPDXNone)):
            messages = messages + [
                'Snippet Concluded License must be one of '
                'document.License, utils.NoAssert or utils.SPDXNone'
            ]

        return messages

    def validate_licenses_in_snippet(self, messages=None):
        if len(self.licenses_in_snippet) == 0:
            messages = messages + ['Snippet must have at least one license in file.']
        else:
            for lic in self.licenses_in_snippet:
                if not isinstance(lic, (document.License, utils.NoAssert,
                                        utils.SPDXNone)):
                    messages = messages + [
                        'Licenses in Snippet must be one of '
                        'document.License, utils.NoAssert or utils.SPDXNone'
                    ]

        return messages

    def has_optional_field(self, field):
        return getattr(self, field, None) is not None
