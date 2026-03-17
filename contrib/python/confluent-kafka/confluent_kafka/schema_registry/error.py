#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
try:
    from fastavro.schema import SchemaParseException, UnknownType
except ImportError:
    pass

__all__ = ['SchemaRegistryError', 'SchemaParseException', 'UnknownType']


class SchemaRegistryError(Exception):
    """
    Represents an error returned by the Confluent Schema Registry

    Args:
        http_status_code (int): HTTP status code

        error_code (int): Schema Registry error code; -1 represents an unknown
            error.

        error_message (str): Description of the error

    See Also:
        `API Error Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#errors>`_

    """  # noqa: E501
    UNKNOWN = -1

    def __init__(self, http_status_code, error_code, error_message):
        self.http_status_code = http_status_code
        self.error_code = error_code
        self.error_message = error_message

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "{} (HTTP status code {}, SR code {})".format(self.error_message,
                                                             self.http_status_code,
                                                             self.error_code)
