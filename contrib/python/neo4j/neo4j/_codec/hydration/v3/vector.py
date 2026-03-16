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


from ....vector import (
    Vector,
    VectorDType,
)
from ...packstream import Structure


_DTYPE_LOOKUP = {
    b"\xc8": VectorDType.I8,
    b"\xc9": VectorDType.I16,
    b"\xca": VectorDType.I32,
    b"\xcb": VectorDType.I64,
    b"\xc6": VectorDType.F32,
    b"\xc1": VectorDType.F64,
}

_TYPE_LOOKUP = {v: k for k, v in _DTYPE_LOOKUP.items()}


def hydrate_vector(typ: bytes, data: bytes) -> Vector:
    """
    Hydrator for `Vector` values.

    :param typ: bytes marking the inner type of the vector
    :param data: big-endian bytes representing the vector data
    :returns: Vector
    """
    dtype = _DTYPE_LOOKUP[typ]
    return Vector(data, dtype)


def dehydrate_vector(value: Vector) -> Structure:
    """
    Dehydrator for `Vector` values.

    :param value:
    :type value: Vector
    :returns:
    """
    return Structure(b"V", _TYPE_LOOKUP[value.dtype], value.raw())
