#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import Any, Optional, Union
from urllib.request import urlopen

from xmlschema.names import XSD_NAMESPACE
from xmlschema.aliases import NsmapType, NormalizedLocationsType, \
    LocationsType, XMLSourceType, UriMapperType
from xmlschema.exceptions import XMLResourceError, XMLResourceOSError, XMLSchemaValueError
from xmlschema.utils.urls import normalize_url

from .xml_resource import XMLResource


def fetch_resource(location: str, base_url: Optional[str] = None, timeout: int = 30) -> str:
    """
    Fetches a resource by trying to access it. If the resource is accessible
    returns its normalized URL, otherwise raises an `XMLResourceOSError`.

    :param location: a URL or a file path.
    :param base_url: reference base URL for normalizing local and relative URLs.
    :param timeout: the timeout in seconds for the connection attempt in case of remote data.
    :return: a normalized URL.
    """
    if not location:
        raise XMLSchemaValueError("the 'location' argument must contain a not empty string")

    url = normalize_url(location, base_url)
    try:
        with urlopen(url, timeout=timeout):
            return url
    except OSError as err:
        if url == normalize_url(location):
            raise XMLResourceOSError(err)

        # fallback using the location without a base URL
        alt_url = normalize_url(location)
        try:
            with urlopen(alt_url, timeout=timeout):
                return alt_url
        except OSError:
            raise XMLResourceOSError(err) from err


def fetch_schema_locations(source: Union['XMLResource', XMLSourceType],
                           locations: Optional[LocationsType] = None,
                           base_url: Optional[str] = None,
                           allow: str = 'all',
                           defuse: str = 'remote',
                           timeout: int = 30,
                           uri_mapper: Optional[UriMapperType] = None,
                           root_only: bool = True,
                           **_kwargs: Any) -> tuple[str, NormalizedLocationsType]:
    """
    Fetches schema location hints from an XML data source and a list of location hints.
    If an accessible schema location is not found raises a ValueError.

    :param source: can be an :class:`XMLResource` instance, a file-like object a path \
    to a file or a URI of a resource or an Element instance or an ElementTree instance or \
    a string containing the XML data. If the passed argument is not an :class:`XMLResource` \
    instance a new one is built using this and *defuse*, *timeout* and *lazy* arguments.
    :param locations: a dictionary or dictionary items with additional schema location hints.
    :param base_url: the same argument of the :class:`XMLResource`.
    :param allow: the same argument of the :class:`XMLResource`, \
    applied to location hints only.
    :param defuse: the same argument of the :class:`XMLResource`.
    :param timeout: the same argument of the :class:`XMLResource` but with a reduced default.
    :param uri_mapper: an optional argument for building the schema from location hints.
    :param root_only: if `True` extracts from the XML source only the location hints \
    of the root element.
    :param _kwargs: unused keyword arguments.
    :return: A 2-tuple with the URL referring to the first reachable schema resource \
    and a list of dictionary items with normalized location hints.
    """
    if not isinstance(source, XMLResource):
        resource = XMLResource(source, base_url, defuse=defuse, timeout=timeout, lazy=True)
    else:
        resource = source

    locations = resource.get_locations(locations, root_only=root_only)
    if not locations:
        raise XMLSchemaValueError("provided arguments don't contain any schema location hint")

    namespace = resource.namespace
    for ns, location in sorted(locations, key=lambda x: x[0] != namespace):
        try:
            resource = XMLResource(location, base_url, allow, defuse, timeout,
                                   lazy=True, uri_mapper=uri_mapper)
        except (XMLResourceError, OSError, SyntaxError):
            continue

        if resource.namespace == XSD_NAMESPACE and resource.url:
            return resource.url, locations
    else:
        raise XMLSchemaValueError("not found a schema for provided XML source")


def fetch_schema(source: Union['XMLResource', XMLSourceType],
                 locations: Optional[LocationsType] = None,
                 base_url: Optional[str] = None,
                 allow: str = 'all',
                 defuse: str = 'remote',
                 timeout: int = 30,
                 uri_mapper: Optional[UriMapperType] = None,
                 root_only: bool = True,
                 **_kwargs: Any) -> str:
    """
    Like :meth:`fetch_schema_locations` but returns only the URL of a loadable XSD
    schema from location hints fetched from the source or provided by argument.
    """
    return fetch_schema_locations(source, locations, base_url, allow,
                                  defuse, timeout, uri_mapper, root_only)[0]


def fetch_namespaces(source: XMLSourceType,
                     base_url: Optional[str] = None,
                     allow: str = 'all',
                     defuse: str = 'remote',
                     timeout: int = 30,
                     root_only: bool = False,
                     **_kwargs: Any) -> NsmapType:
    """
    Fetches namespaces information from the XML data source. The argument *source*
    can be a string containing the XML document or file path or an url or a file-like
    object or an ElementTree instance or an Element instance. A dictionary with
    namespace mappings is returned.
    """
    resource = XMLResource(source, base_url, allow, defuse, timeout, lazy=True)
    return resource.get_namespaces(root_only=root_only)
