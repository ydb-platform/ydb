#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import Optional

from xmlschema.names import XSD_NOTATION
from xmlschema.translation import gettext as _
from xmlschema.utils.qnames import get_qname

from .xsdbase import XsdComponent


class XsdNotation(XsdComponent):
    """
    Class for XSD *notation* declarations.

    ..  <notation
          id = ID
          name = NCName
          public = token
          system = anyURI
          {any attributes with non-schema namespace}...>
          Content: (annotation?)
        </notation>
    """
    _ADMITTED_TAGS = XSD_NOTATION,

    def _parse(self) -> None:
        if self.parent is not None:
            self.parse_error(_("a notation declaration must be global"))
        try:
            self.name = get_qname(self.target_namespace, self.elem.attrib['name'])
        except KeyError:
            self.parse_error(_("a notation must have a 'name' attribute"))

        if 'public' not in self.elem.attrib and 'system' not in self.elem.attrib:
            self.parse_error(_("a notation must have a 'public' or a 'system' attribute"))

    @property
    def public(self) -> Optional[str]:
        return self.elem.get('public')

    @property
    def system(self) -> Optional[str]:
        return self.elem.get('system')
