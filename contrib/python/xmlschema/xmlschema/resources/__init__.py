#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from .xml_resource import XMLResourceManager, XMLResource
from .parsers import iterfind_parser, limited_parser
from .fetchers import fetch_resource, fetch_namespaces, \
    fetch_schema_locations, fetch_schema

__all__ = ['XMLResourceManager', 'XMLResource', 'iterfind_parser',
           'limited_parser', 'fetch_resource',
           'fetch_namespaces', 'fetch_schema_locations', 'fetch_schema']
