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

import hashlib

from six.moves import reduce

from spdx import checksum
from spdx import creationinfo
from spdx import document
from spdx import utils


class Package(object):

    """
    Represent an analyzed Package.
    Fields:
     - name : Mandatory, string.
     - spdx_id: Uniquely identify any element in an SPDX document which may be
     referenced by other elements. Mandatory, one. Type: str.
     - version: Optional, string.
     - file_name: Optional, string.
     - supplier: Optional, Organization or Person or NO_ASSERTION.
     - originator: Optional, Organization or Person.
     - download_location: Mandatory, URL as string.
     - files_analyzed: Indicates whether the file content of this package has
     been available for or subjected to analysis when creating the SPDX
     document. If "false" indicates packages that represent metadata or URI
     references to a project, product, artifact, distribution or a component.
     If set to "false", the package must not contain any files.
     Optional, boolean.
     - homepage: Optional, URL as string or NONE or NO_ASSERTION.
     - verif_code: Mandatory string.
     - check_sum: Optional , spdx.checksum.Algorithm.
     - source_info: Optional string.
     - conc_lics: Mandatory spdx.document.License or spdx.utils.SPDXNone or
     - spdx.utils.NoAssert.
     - license_declared : Mandatory spdx.document.License or spdx.utils.SPDXNone or
     - spdx.utils.NoAssert.
     - license_comment  : optional string.
     - licenses_from_files: list of spdx.document.License or spdx.utils.SPDXNone or
     - spdx.utils.NoAssert.
     - cr_text: Copyright text, string , utils.NoAssert or utils.SPDXNone. Mandatory.
     - summary: Optional str.
     - description: Optional str.
     - comment: Comments about the package being described, optional one.
     Type: str
     - files: List of files in package, atleast one.
     - verif_exc_files : list of file names excluded from verification code or None.
     - ext_pkg_refs : External references referenced within the given package.
     Optional, one or many. Type: ExternalPackageRef
    """

    def __init__(self, name=None, spdx_id=None, download_location=None,
                 version=None, file_name=None, supplier=None, originator=None):
        self.name = name
        self.spdx_id = spdx_id
        self.version = version
        self.file_name = file_name
        self.supplier = supplier
        self.originator = originator
        self.download_location = download_location
        self.files_analyzed = None
        self.homepage = None
        self.verif_code = None
        self.check_sum = None
        self.source_info = None
        self.conc_lics = None
        self.license_declared = None
        self.license_comment = None
        self.licenses_from_files = []
        self.cr_text = None
        self.summary = None
        self.description = None
        self.comment = None
        self.files = []
        self.verif_exc_files = []
        self.pkg_ext_refs = []

    def add_file(self, fil):
        self.files.append(fil)

    def add_lics_from_file(self, lics):
        self.licenses_from_files.append(lics)

    def add_exc_file(self, filename):
        self.verif_exc_files.append(filename)

    def add_pkg_ext_refs(self, pkg_ext_ref):
        self.pkg_ext_refs.append(pkg_ext_ref)

    def validate(self, messages=None):
        """
        Validate the package fields.
        Append user friendly error messages to the `messages` list.
        """
        messages = self.validate_checksum(messages)
        messages = self.validate_optional_str_fields(messages)
        messages = self.validate_mandatory_str_fields(messages)
        messages = self.validate_files(messages)
        messages = self.validate_pkg_ext_refs(messages)
        messages = self.validate_mandatory_fields(messages)
        messages = self.validate_optional_fields(messages)

        return messages

    def validate_optional_fields(self, messages):
        if self.originator and not isinstance(self.originator, (utils.NoAssert, creationinfo.Creator)):
            messages = messages + [
                'Package originator must be instance of '
                'spdx.utils.NoAssert or spdx.creationinfo.Creator'
            ]

        if self.supplier and not isinstance(self.supplier, (utils.NoAssert, creationinfo.Creator)):
            messages = messages + [
                'Package supplier must be instance of '
                'spdx.utils.NoAssert or spdx.creationinfo.Creator'
            ]

        return messages

    def validate_pkg_ext_refs(self, messages=None):
        for ref in self.pkg_ext_refs:
            if isinstance(ref, ExternalPackageRef):
                messages = ref.validate(messages)
            else:
                messages = messages + [
                    'External package references must be of the type '
                    'spdx.package.ExternalPackageRef and not ' + str(type(ref))
                ]

        return messages

    def validate_mandatory_fields(self, messages):
        if not isinstance(self.conc_lics, (utils.SPDXNone, utils.NoAssert, document.License)):
            messages = messages + [
                'Package concluded license must be instance of '
                'spdx.utils.SPDXNone or spdx.utils.NoAssert or '
                'spdx.document.License'
            ]

        if not isinstance(self.license_declared, (utils.SPDXNone, utils.NoAssert, document.License)):
            messages = messages + [
                'Package declared license must be instance of '
                'spdx.utils.SPDXNone or spdx.utils.NoAssert or '
                'spdx.document.License'
            ]

        # FIXME: this is obscure and unreadable
        license_from_file_check = lambda prev, el: prev and isinstance(el, (document.License, utils.SPDXNone, utils.NoAssert))
        if not reduce(license_from_file_check, self.licenses_from_files, True):
            messages = messages + [
                'Each element in licenses_from_files must be instance of '
                'spdx.utils.SPDXNone or spdx.utils.NoAssert or '
                'spdx.document.License'
            ]

        if not self.licenses_from_files:
            messages = messages + [
                'Package licenses_from_files can not be empty'
            ]

        return messages

    def validate_files(self, messages):
        if not self.files:
            messages = messages + [
                'Package must have at least one file.'
            ]
        else:
            for f in self.files:
                messages = f.validate(messages)

        return messages

    def validate_optional_str_fields(self, messages):
        """Fields marked as optional and of type string in class
        docstring must be of a type that provides __str__ method.
        """
        FIELDS = [
            'file_name',
            'version',
            'homepage',
            'source_info',
            'summary',
            'description',
            'comment'
        ]
        messages = self.validate_str_fields(FIELDS, True, messages)

        return messages

    def validate_mandatory_str_fields(self, messages):
        """Fields marked as Mandatory and of type string in class
        docstring must be of a type that provides __str__ method.
        """
        FIELDS = ['name', 'spdx_id', 'download_location', 'verif_code', 'cr_text']
        messages = self.validate_str_fields(FIELDS, False, messages)

        return messages

    def validate_str_fields(self, fields, optional, messages):
        """Helper for validate_mandatory_str_field and
        validate_optional_str_fields"""
        for field_str in fields:
            field = getattr(self, field_str)
            if field is not None:
                # FIXME: this does not make sense???
                attr = getattr(field, '__str__', None)
                if not callable(attr):
                    messages = messages + [
                        '{0} must provide __str__ method.'.format(field)
                    ]
                    # Continue checking.
            elif not optional:
                messages = messages + [
                    'Package {0} can not be None.'.format(field_str)
                ]

        return messages

    def validate_checksum(self, messages):
        if not isinstance(self.check_sum, checksum.Algorithm):
            messages = messages + [
                'Package checksum must be instance of spdx.checksum.Algorithm'
            ]
        else:
            if self.check_sum.identifier != 'SHA1':
                messages = messages + ['File checksum algorithm must be SHA1']

        return messages

    def calc_verif_code(self):
        hashes = []

        for file_entry in self.files:
            if (isinstance(file_entry.chk_sum, checksum.Algorithm) and
                file_entry.chk_sum.identifier == 'SHA1'):
                sha1 = file_entry.chk_sum.value
            else:
                sha1 = file_entry.calc_chksum()
            hashes.append(sha1)

        hashes.sort()

        sha1 = hashlib.sha1()
        sha1.update(''.join(hashes).encode('utf-8'))
        return sha1.hexdigest()

    def has_optional_field(self, field):
        return getattr(self, field, None) is not None


class ExternalPackageRef(object):
    """
    An External Reference allows a Package to reference an external source of
    additional information, metadata, enumerations, asset identifiers, or
    downloadable content believed to be relevant to the Package.
    Fields:
    - category: "SECURITY" or "PACKAGE-MANAGER" or "OTHER".
    - pkg_ext_ref_type: A unique string containing letters, numbers, ".","-".
    - locator: A unique string with no spaces necessary to access the
    package-specific information, metadata, or content within the target
    location.
    - comment: To provide information about the purpose and target of the
    reference.
    """

    def __init__(self, category=None, pkg_ext_ref_type=None, locator=None,
                 comment=None):
        self.category = category
        self.pkg_ext_ref_type = pkg_ext_ref_type
        self.locator = locator
        self.comment = comment

    def validate(self, messages=None):
        """
        Validate all fields of the ExternalPackageRef class and update the
        messages list with user friendly error messages for display.
        """
        messages = self.validate_category(messages)
        messages = self.validate_pkg_ext_ref_type(messages)
        messages = self.validate_locator(messages)

        return messages

    def validate_category(self, messages=None):
        if self.category is None:
            messages = messages + ['ExternalPackageRef has no category.']

        return messages

    def validate_pkg_ext_ref_type(self, messages=None):
        if self.pkg_ext_ref_type is None:
            messages = messages + ['ExternalPackageRef has no type.']

        return messages

    def validate_locator(self, messages=None):
        if self.locator is None:
            messages = messages + ['ExternalPackageRef has no locator.']

        return messages
