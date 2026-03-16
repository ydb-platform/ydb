#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""Package protection limits. Values can be changed after import to set different limits."""
import sys
from types import ModuleType
from typing import Any

from xmlschema.translation import gettext as _
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError
from xmlschema import _limits


class LimitsModule(ModuleType):
    def __setattr__(self, attr: str, value: Any) -> None:
        if attr not in ('MAX_MODEL_DEPTH', 'MAX_SCHEMA_SOURCES',
                        'MAX_XML_DEPTH', 'MAX_XML_ELEMENTS'):
            pass
        elif not isinstance(value, int):
            raise XMLSchemaTypeError(_('Value {!r} is not an int').format(value))
        elif attr == 'MAX_MODEL_DEPTH':
            if value < 5:
                raise XMLSchemaValueError(_('{} limit must be at least 5').format(attr))
            _limits.MAX_MODEL_DEPTH = value
        elif attr == 'MAX_SCHEMA_SOURCES':
            if value < 10:
                raise XMLSchemaValueError(_('{} limit must be at least 10').format(attr))
            _limits.MAX_SCHEMA_SOURCES = value
        elif value < 1:
            raise XMLSchemaValueError(_('{} limit must be at least 1').format(attr))
        else:
            setattr(_limits, attr, value)

        super().__setattr__(attr, value)


sys.modules[__name__].__class__ = LimitsModule


MAX_MODEL_DEPTH = 15
"""
Maximum XSD model group depth. An `XMLSchemaModelDepthError` is raised if
this limit is exceeded.
"""

MAX_SCHEMA_SOURCES = 1000
"""
Maximum number of XSD schema sources loadable by each `XsdGlobals` instance.
An `XMLSchemaValidatorError` is raised if this limit is exceeded.
"""

MAX_XML_DEPTH = 1000
"""
Maximum depth of XML data. An `XMLResourceExceeded` is raised if this limit is exceeded.
"""

MAX_XML_ELEMENTS = 10 ** 6
"""
Maximum number of XML elements allowed in a XML document. An `XMLResourceExceeded`
is raised if this limit is exceeded. Not affects lazy resources.
"""
