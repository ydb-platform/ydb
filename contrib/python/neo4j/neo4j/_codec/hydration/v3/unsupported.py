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


from .... import _typing as t
from ....types import UnsupportedType


def hydrate_unsupported(
    name: str,
    min_bolt_major: int,
    min_bolt_minor: int,
    extra: dict[str, t.Any],
) -> UnsupportedType:
    """
    Hydrator for `UnsupportedType` values.

    :param name: name of the type
    :param min_bolt_major: minimum major version of the Bolt protocol
        supporting this type
    :param min_bolt_minor: minimum minor version of the Bolt protocol
        supporting this type
    :param extra: dict containing optional "message" key
    :returns: UnsupportedType instance
    """
    return UnsupportedType._new(
        name,
        (min_bolt_major, min_bolt_minor),
        extra.get("message"),
    )
