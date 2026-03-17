# Copyright 2022 Confluent Inc.
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

from enum import Enum


class ConversionUtil:
    @staticmethod
    def convert_to_enum(val, enum_clazz):
        if type(enum_clazz) is not type(Enum):
            raise TypeError("'enum_clazz' must be of type Enum")

        if isinstance(val, str):
            # Allow it to be specified as case-insensitive string, for convenience.
            try:
                val = enum_clazz[val.upper()]
            except KeyError:
                raise ValueError("Unknown value \"%s\": should be a %s" % (val, enum_clazz.__name__))

        elif isinstance(val, int):
            # The C-code passes restype as an int, convert to enum.
            val = enum_clazz(val)

        elif not isinstance(val, enum_clazz):
            raise TypeError("Unknown value \"%s\": should be a %s" % (val, enum_clazz.__name__))

        return val
