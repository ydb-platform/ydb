# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2020 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
"""
Type Adapter to convert Python-native types into CEL structures.

Currently, Atomic Python objects have direct use of types in :mod:`celpy.celtypes`.

Non-Atomic Python objects are characterized by JSON and Protobuf
objects. This module has functions to convert JSON objects to CEL.

The protobuf decoder is TBD.

A more sophisticated type injection capability may be needed to permit
additional types or extensions to :mod:`celpy.celtypes`.
"""

import base64
import datetime
import json
from typing import Any, Dict, List, Union, cast

from celpy import celtypes

JSON = Union[Dict[str, Any], List[Any], bool, float, int, str, None]


class CELJSONEncoder(json.JSONEncoder):
    """
    An Encoder to export CEL objects as JSON text.

    This is **not** a reversible transformation. Some things are coerced to strings
    without any more detailed type marker.
    Specifically timestamps, durations, and bytes.
    """

    @staticmethod
    def to_python(
        cel_object: celtypes.Value,
    ) -> Union[celtypes.Value, List[Any], Dict[Any, Any], bool]:
        """Recursive walk through the CEL object, replacing BoolType with native bool instances.
        This lets the :py:mod:`json` module correctly represent the obects
        with JSON ``true`` and ``false``.

        This will also replace ListType and MapType with native ``list`` and ``dict``.
        All other CEL objects will be left intact. This creates an intermediate hybrid
        beast that's not quite a :py:class:`celtypes.Value` because a few things have been replaced.
        """
        if isinstance(cel_object, celtypes.BoolType):
            return True if cel_object else False
        elif isinstance(cel_object, celtypes.ListType):
            return [CELJSONEncoder.to_python(item) for item in cel_object]
        elif isinstance(cel_object, celtypes.MapType):
            return {
                CELJSONEncoder.to_python(key): CELJSONEncoder.to_python(value)
                for key, value in cel_object.items()
            }
        else:
            return cel_object

    def encode(self, cel_object: celtypes.Value) -> str:
        """
        Override built-in encode to create proper Python :py:class:`bool` objects.
        """
        return super().encode(CELJSONEncoder.to_python(cel_object))

    def default(self, cel_object: celtypes.Value) -> JSON:
        if isinstance(cel_object, celtypes.TimestampType):
            return str(cel_object)
        elif isinstance(cel_object, celtypes.DurationType):
            return str(cel_object)
        elif isinstance(cel_object, celtypes.BytesType):
            return base64.b64encode(cel_object).decode("ASCII")
        else:
            return cast(JSON, super().default(cel_object))


class CELJSONDecoder(json.JSONDecoder):
    """
    An Encoder to import CEL objects from JSON to the extent possible.

    This does not handle non-JSON types in any form. Coercion from string
    to TimestampType or DurationType or BytesType is handled by celtype
    constructors.
    """

    def decode(self, source: str, _w: Any = None) -> Any:
        raw_json = super().decode(source)
        return json_to_cel(raw_json)


def json_to_cel(document: JSON) -> celtypes.Value:
    """Convert parsed JSON object from Python to CEL to the extent possible.

    It's difficult to distinguish strings which should be timestamps or durations.

    ::

        >>> from pprint import pprint
        >>> from celpy.adapter import json_to_cel
        >>> doc = json.loads('["str", 42, 3.14, null, true, {"hello": "world"}]')
        >>> cel = json_to_cel(doc)
        >>> pprint(cel)
        ListType([StringType('str'), IntType(42), DoubleType(3.14), None, BoolType(True), \
MapType({StringType('hello'): StringType('world')})])
    """
    if isinstance(document, bool):
        return celtypes.BoolType(document)
    elif isinstance(document, float):
        return celtypes.DoubleType(document)
    elif isinstance(document, int):
        return celtypes.IntType(document)
    elif isinstance(document, str):
        return celtypes.StringType(document)
    elif document is None:
        return None
    elif isinstance(document, (tuple, List)):
        return celtypes.ListType([json_to_cel(item) for item in document])
    elif isinstance(document, Dict):
        return celtypes.MapType(
            {json_to_cel(key): json_to_cel(value) for key, value in document.items()}
        )
    elif isinstance(document, datetime.datetime):
        return celtypes.TimestampType(document)
    elif isinstance(document, datetime.timedelta):
        return celtypes.DurationType(document)
    else:
        raise ValueError(
            f"unexpected type {type(document)} in JSON structure {document!r}"
        )
