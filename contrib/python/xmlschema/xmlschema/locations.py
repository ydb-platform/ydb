#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import importlib.resources
import pathlib
from collections.abc import Iterable
from typing import Optional, Any, MutableMapping, Iterator, TypeVar

from xmlschema.aliases import LocationsMapType, LocationsType
from xmlschema.exceptions import XMLSchemaTypeError
from xmlschema.translation import gettext as _
from xmlschema.utils.urls import normalize_locations
import xmlschema.names as nm

T = TypeVar('T', bound=object)


class NamespaceResourcesMap(MutableMapping[str, list[T]]):
    """
    Dictionary for storing information about namespace resources. Values are
    lists of objects. Setting an existing value appends the object to the value.
    Setting a value with a list sets/replaces the value.
    """
    __slots__ = ('_store',)

    def __init__(self, *args: Any, **kwargs: Any):
        self._store: dict[str, list[T]] = {}
        for item in args:
            self.update(item)
        self.update(kwargs)

    def __getitem__(self, uri: str) -> list[T]:
        return self._store[uri]

    def __setitem__(self, uri: str, value: Any) -> None:
        if isinstance(value, list):
            self._store[uri] = value[:]
        else:
            try:
                self._store[uri].append(value)
            except KeyError:
                self._store[uri] = [value]

    def __delitem__(self, uri: str) -> None:
        del self._store[uri]

    def __iter__(self) -> Iterator[str]:
        return iter(self._store)

    def __len__(self) -> int:
        return len(self._store)

    def __repr__(self) -> str:
        return repr(self._store)

    def clear(self) -> None:
        self._store.clear()

    def copy(self) -> 'NamespaceResourcesMap[T]':
        obj: NamespaceResourcesMap[T] = object.__new__(self.__class__)
        obj._store = {k: v.copy() for k, v in self.items()}
        return obj

    __copy__ = copy


def get_locations(locations: Optional[LocationsType], base_url: Optional[str] = None) \
        -> NamespaceResourcesMap[str]:
    """Returns a NamespaceResourcesMap with location hints provided at schema initialization."""
    if locations is None:
        return NamespaceResourcesMap()
    elif isinstance(locations, NamespaceResourcesMap):
        return locations
    elif isinstance(locations, tuple):
        return NamespaceResourcesMap(locations)
    elif not isinstance(locations, Iterable):
        msg = _('wrong type {!r} for locations argument')
        raise XMLSchemaTypeError(msg.format(type(locations)))
    else:
        return NamespaceResourcesMap(normalize_locations(locations, base_url))


SCHEMAS_DIR = importlib.resources.files(__package__) / "schemas"

###
# Standard locations for well-known namespaces
LOCATIONS: LocationsMapType = {
    nm.XSD_NAMESPACE: [
        "https://www.w3.org/2001/XMLSchema.xsd",  # XSD 1.0
        "https://www.w3.org/2009/XMLSchema/XMLSchema.xsd",  # Mutable XSD 1.1
        "https://www.w3.org/2012/04/XMLSchema.xsd"
    ],
    nm.XML_NAMESPACE: "https://www.w3.org/2001/xml.xsd",
    nm.XSI_NAMESPACE: "https://www.w3.org/2001/XMLSchema-instance",
    nm.XSLT_NAMESPACE: "https://www.w3.org/2007/schema-for-xslt20.xsd",
    nm.HFP_NAMESPACE: "https://www.w3.org/2001/XMLSchema-hasFacetAndProperty",
    nm.VC_NAMESPACE: "https://www.w3.org/2007/XMLSchema-versioning/XMLSchema-versioning.xsd",
    nm.XLINK_NAMESPACE: "https://www.w3.org/1999/xlink.xsd",
    nm.WSDL_NAMESPACE: "https://schemas.xmlsoap.org/wsdl/",
    nm.SOAP_NAMESPACE: "https://schemas.xmlsoap.org/wsdl/soap/",
    nm.SOAP_ENVELOPE_NAMESPACE: "https://schemas.xmlsoap.org/soap/envelope/",
    nm.SOAP_ENCODING_NAMESPACE: "https://schemas.xmlsoap.org/soap/encoding/",
    nm.DSIG_NAMESPACE: "https://www.w3.org/2000/09/xmldsig#",
    nm.DSIG11_NAMESPACE: "https://www.w3.org/2009/xmldsig11#",
    nm.XENC_NAMESPACE: "https://www.w3.org/TR/xmlenc-core/xenc-schema.xsd",
    nm.XENC11_NAMESPACE: "https://www.w3.org/TR/xmlenc-core1/xenc-schema-11.xsd",
}

# Fallback locations for well-known namespaces
FALLBACK_LOCATIONS: LocationsMapType = {
    nm.XSD_NAMESPACE: [
        SCHEMAS_DIR.joinpath('XSD_1.0', 'XMLSchema.xsd'),
        SCHEMAS_DIR.joinpath('XSD_1.1', 'XMLSchema.xsd'),
        SCHEMAS_DIR.joinpath('XSD_1.1', 'XMLSchema.xsd'),
    ],
    nm.XML_NAMESPACE: SCHEMAS_DIR.joinpath('XML', 'xml.xsd'),
    nm.XSI_NAMESPACE: SCHEMAS_DIR.joinpath('XSI', 'XMLSchema-instance.xsd'),
    nm.HFP_NAMESPACE: SCHEMAS_DIR.joinpath('HFP', 'XMLSchema-hasFacetAndProperty.xsd'),
    nm.VC_NAMESPACE: SCHEMAS_DIR.joinpath('VC', 'XMLSchema-versioning.xsd'),
    nm.XLINK_NAMESPACE: SCHEMAS_DIR.joinpath('XLINK', 'xlink.xsd'),
    nm.XHTML_NAMESPACE: SCHEMAS_DIR.joinpath('XHTML', 'xhtml1-strict.xsd'),
    nm.WSDL_NAMESPACE: SCHEMAS_DIR.joinpath('WSDL', 'wsdl.xsd'),
    nm.SOAP_NAMESPACE: SCHEMAS_DIR.joinpath('WSDL', 'wsdl-soap.xsd'),
    nm.SOAP_ENVELOPE_NAMESPACE: SCHEMAS_DIR.joinpath('WSDL', 'soap-envelope.xsd'),
    nm.SOAP_ENCODING_NAMESPACE: SCHEMAS_DIR.joinpath('WSDL', 'soap-encoding.xsd'),
    nm.DSIG_NAMESPACE: SCHEMAS_DIR.joinpath('DSIG', 'xmldsig-core-schema.xsd'),
    nm.DSIG11_NAMESPACE: SCHEMAS_DIR.joinpath('DSIG', 'xmldsig11-schema.xsd'),
    nm.XENC_NAMESPACE: SCHEMAS_DIR.joinpath('XENC', 'xenc-schema.xsd'),
    nm.XENC11_NAMESPACE: SCHEMAS_DIR.joinpath('XENC', 'xenc-schema-11.xsd'),
}
