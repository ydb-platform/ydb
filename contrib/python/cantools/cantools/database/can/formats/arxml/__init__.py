__all__ = ["AutosarBusSpecifics", "AutosarDatabaseSpecifics",
           "AutosarEnd2EndProperties", "AutosarMessageSpecifics",
           "AutosarNodeSpecifics", "AutosarSecOCProperties", "load_string"]

import re
from typing import Any
from xml.etree import ElementTree

from ....utils import sort_signals_by_start_bit, type_sort_signals
from ...internal_database import InternalDatabase
from .bus_specifics import AutosarBusSpecifics
from .database_specifics import AutosarDatabaseSpecifics
from .ecu_extract_loader import EcuExtractLoader
from .end_to_end_properties import AutosarEnd2EndProperties
from .message_specifics import AutosarMessageSpecifics
from .node_specifics import AutosarNodeSpecifics
from .secoc_properties import AutosarSecOCProperties
from .system_loader import SystemLoader


def is_ecu_extract(root: Any # For whatever reason, mypy does not
                             # accept 'ElementTree' here...
                   ) -> bool:
    """Given the root object of an ARXML file's ElementTree,
    determine if the file represents an ECU extract.

    If it is not, it probably represents a system. Be aware that
    currently loading ECU extracts is only supported for AUTOSAR 4.
    """

    ecuc_value_collection_xpath = \
        './ns:AR-PACKAGES' + \
        '/ns:AR-PACKAGE' + \
        '/ns:ELEMENTS' + \
        '/ns:ECUC-VALUE-COLLECTION'

    namespaces = { 'ns': 'http://autosar.org/schema/r4.0' }

    ecuc_value_collection = \
        root.find(ecuc_value_collection_xpath, namespaces)

    return ecuc_value_collection is not None

def load_string(string:str,
                strict:bool=True,
                sort_signals:type_sort_signals=sort_signals_by_start_bit) \
            -> InternalDatabase:
    """Parse given ARXML format string.

    """

    root = ElementTree.fromstring(string)

    m = re.match(r'{(.*)}AUTOSAR', root.tag)
    if not m:
        raise ValueError(f"No XML namespace specified or illegal root tag name '{root.tag}'")
    xml_namespace = m.group(1)

    # Should be replaced with a validation using the XSD file.
    recognized_namespace = False
    if re.match(r'http://autosar.org/schema/r(4.*)', xml_namespace) \
       or re.match(r'http://autosar.org/(3.*)', xml_namespace) \
       or re.match(r'http://autosar.org/(.*)\.DAI\.[0-9]', xml_namespace):
        recognized_namespace = True

    if not recognized_namespace:
        raise ValueError(f"Unrecognized XML namespace '{xml_namespace}'")

    if is_ecu_extract(root):
        expected_root = f'{{{xml_namespace}}}AUTOSAR'
        if root.tag != expected_root:
            raise ValueError(f'Expected root element tag {expected_root}, '
                             f'but got {root.tag}.')

        return EcuExtractLoader(root, strict, sort_signals).load()
    else:
        return SystemLoader(root, strict, sort_signals).load()
