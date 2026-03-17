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


from __future__ import annotations as _

from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Generator,
    Hashable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    Sequence,
    Set,
    Sized,
    ValuesView,
)
from importlib.util import find_spec as _find_spec
from typing import (
    Any,
    cast,
    ClassVar,
    Concatenate,
    Final,
    Generic,
    Literal,
    NamedTuple,
    overload,
    ParamSpec,
    Protocol,
    SupportsIndex,
    TextIO,
    TYPE_CHECKING,
    TypeAlias,
    TypedDict,
    TypeVar,
    Union,
)


__all__: tuple[str, ...] = (
    "TYPE_CHECKING",
    "Any",
    "AsyncIterator",
    "Awaitable",
    "Callable",
    "ClassVar",
    "Collection",
    "Concatenate",
    "Final",
    "Generator",
    "Generic",
    "Hashable",
    "ItemsView",
    "Iterable",
    "Iterator",
    "KeysView",
    "Literal",
    "Mapping",
    "NamedTuple",
    "ParamSpec",
    "Protocol",
    "Sequence",
    "Set",
    "Sized",
    "SupportsIndex",
    "TextIO",
    "TypeAlias",
    "TypeVar",
    "TypedDict",
    "Union",
    "ValuesView",
    "cast",
    "overload",
)


_te_available = _find_spec("typing_extensions") is not None

if TYPE_CHECKING or _te_available:
    from typing_extensions import LiteralString  # Python 3.11+
    from typing_extensions import NotRequired  # Python 3.11+
    from typing_extensions import Self  # Python 3.11+

    __all__ = (  # noqa: PLE0604 false positive
        *__all__,
        "LiteralString",
        "Self",
        "NotRequired",
    )
