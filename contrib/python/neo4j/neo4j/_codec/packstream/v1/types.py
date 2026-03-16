# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from ...._optional_deps import (
    np,
    pd,
)


NONE_VALUES: tuple = (None,)
TRUE_VALUES: tuple = (True,)
FALSE_VALUES: tuple = (False,)
INT_TYPES: tuple[type, ...] = (int,)
FLOAT_TYPES: tuple[type, ...] = (float,)
# we can't put tuple here because spatial types subclass tuple,
# and we don't want to treat them as sequences
SEQUENCE_TYPES: tuple[type, ...] = (list,)
MAPPING_TYPES: tuple[type, ...] = (dict,)
BYTES_TYPES: tuple[type, ...] = (bytes, bytearray)


if np is not None:
    TRUE_VALUES = (*TRUE_VALUES, np.bool_(True))
    FALSE_VALUES = (*FALSE_VALUES, np.bool_(False))
    INT_TYPES = (*INT_TYPES, np.integer)
    FLOAT_TYPES = (*FLOAT_TYPES, np.floating)
    SEQUENCE_TYPES = (*SEQUENCE_TYPES, np.ndarray)

if pd is not None:
    NONE_VALUES = (*NONE_VALUES, pd.NA)
    SEQUENCE_TYPES = (
        *SEQUENCE_TYPES,
        pd.Series,
        pd.Categorical,
        pd.core.arrays.ExtensionArray,
    )
    MAPPING_TYPES = (*MAPPING_TYPES, pd.DataFrame)


__all__ = [
    "BYTES_TYPES",
    "FALSE_VALUES",
    "FLOAT_TYPES",
    "INT_TYPES",
    "MAPPING_TYPES",
    "NONE_VALUES",
    "SEQUENCE_TYPES",
    "TRUE_VALUES",
]
