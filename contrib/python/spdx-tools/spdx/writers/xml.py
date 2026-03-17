
# Copyright (c) the SPDX tools authors
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

import xmltodict

from spdx.writers.tagvalue import InvalidDocumentError
from spdx.writers.jsonyamlxml import Writer


def write_document(document, out, validate=True):

    if validate:
        messages = []
        messages = document.validate(messages)
        if messages:
            raise InvalidDocumentError(messages)

    writer = Writer(document)
    document_object = {'SpdxDocument': writer.create_document()}

    xmltodict.unparse(document_object, out, encoding='utf-8', pretty=True)
