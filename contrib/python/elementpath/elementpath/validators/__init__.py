#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Subpackage for validating against XPath standard schemas.
"""
import pathlib
from xml.etree.ElementTree import Element
from importlib.resources import files
from typing import Optional

from elementpath.exceptions import ElementPathRuntimeError

try:
    import xmlschema
except ImportError:  # pragma: no cover
    from ..exceptions import xpath_error

    def validate_analyzed_string(root: Element) -> None:
        raise ElementPathRuntimeError('not schema-aware')

    def validate_json_to_xml(root: Element) -> None:
        raise xpath_error('FOJS0004')

else:
    from ..namespaces import XPATH_FUNCTIONS_NAMESPACE

    analyzed_string_schema: Optional[xmlschema.XMLSchemaBase] = None
    json_to_xml_schema: Optional[xmlschema.XMLSchemaBase] = None

    __all__ = ['validate_analyzed_string', 'validate_json_to_xml']
    root_node = files(__package__)

    def validate_analyzed_string(root: Element) -> None:
        global analyzed_string_schema

        if analyzed_string_schema is None:
            xsd_file = (root_node / 'analyze-string.xsd').read_text()
            analyzed_string_schema = xmlschema.XMLSchema(xsd_file)

        analyzed_string_schema.validate(root)

    def validate_json_to_xml(root: Element) -> None:
        global json_to_xml_schema

        if json_to_xml_schema is None:
            xsd_file = (root_node / 'schema-for-json.xsd').read_text()
            json_to_xml_schema = xmlschema.XMLSchema(xsd_file)

        json_to_xml_schema.validate(root, namespaces={'j': XPATH_FUNCTIONS_NAMESPACE})
