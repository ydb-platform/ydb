# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


from collections.abc import Mapping
from queue import Queue
from typing import Tuple, Type, Union
from urllib.parse import quote, quote_plus, unquote, urlencode, urlparse

string_types = str, bytes
map = map  # pylint: disable=invalid-name


def to_str(x: Union[str, bytes], encoding: str = "ascii") -> str:
    """
    returns x as a string encoded in "encoding" if it is not already a string
    :param x: the value to convert to a str
    :param encoding: the encoding to convert to - see https://docs.python.org/3/library/codecs.html#standard-encodings
    :return: an encoded str
    """
    if not isinstance(x, str):
        return x.decode(encoding)
    return x


def to_bytes(x: Union[str, bytes], encoding: str = "ascii") -> bytes:
    if not isinstance(x, bytes):
        return x.encode(encoding)
    return x


try:
    reraise_exceptions: Tuple[Type[BaseException], ...] = (RecursionError,)
except NameError:
    reraise_exceptions = ()

try:
    import asyncio

    reraise_exceptions += (asyncio.CancelledError,)
except (ImportError, AttributeError):
    pass


__all__ = [
    "string_types",
    "reraise_exceptions",
    "quote_plus",
    "quote",
    "urlencode",
    "unquote",
    "urlparse",
    "map",
    "Queue",
    "Mapping",
]
