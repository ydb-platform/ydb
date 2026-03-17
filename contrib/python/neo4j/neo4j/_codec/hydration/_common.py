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


from copy import copy
from dataclasses import dataclass

from ... import _typing as t
from ...graph import Graph
from ..packstream import Structure


@dataclass
class DehydrationHooks:
    exact_types: dict[type, t.Callable[[t.Any], t.Any]]
    subtypes: dict[type, t.Callable[[t.Any], t.Any]]

    def update(self, exact_types=None, subtypes=None):
        exact_types = exact_types or {}
        subtypes = subtypes or {}
        self.exact_types.update(exact_types)
        self.subtypes.update(subtypes)

    def extend(self, exact_types=None, subtypes=None):
        exact_types = exact_types or {}
        subtypes = subtypes or {}
        return DehydrationHooks(
            exact_types={**self.exact_types, **exact_types},
            subtypes={**self.subtypes, **subtypes},
        )

    def get_transformer(self, item):
        type_ = type(item)
        transformer = self.exact_types.get(type_)
        if transformer is not None:
            return transformer
        return next(
            (
                f
                for super_type, f in self.subtypes.items()
                if isinstance(item, super_type)
            ),
            None,
        )


class BrokenHydrationObject:
    """
    Represents an object from the server, not understood by the driver.

    A :class:`neo4j.Record` might contain a ``BrokenHydrationObject`` object
    if the driver received data from the server that it did not understand.
    This can for instance happen if the server sends a zoned datetime with a
    zone name unknown to the driver.

    There is no need to explicitly check for this type. Any method on the
    :class:`neo4j.Record` that would return a ``BrokenHydrationObject``, will
    raise a :exc:`neo4j.exceptions.BrokenRecordError`
    with the original exception as cause.
    """

    def __init__(self, error, raw_data):
        self.error = error
        "The exception raised while decoding the received object."
        self.raw_data = raw_data
        """The raw data that caused the exception."""

    def exception_copy(self):
        exc_copy = copy(self.error)
        exc_copy.with_traceback(self.error.__traceback__)
        return exc_copy


class GraphHydrator:
    def __init__(self):
        self.graph = Graph()
        self.struct_hydration_functions = {}


class HydrationScope:
    def __init__(self, hydration_handler, graph_hydrator):
        self._hydration_handler = hydration_handler
        self._graph_hydrator = graph_hydrator
        self._struct_hydration_functions = {
            **hydration_handler.struct_hydration_functions,
            **graph_hydrator.struct_hydration_functions,
        }
        self.hydration_hooks = {
            Structure: self._hydrate_structure,
            list: self._hydrate_list,
            dict: self._hydrate_dict,
        }
        self.dehydration_hooks = hydration_handler.dehydration_hooks

    def _hydrate_structure(self, value):
        f = self._struct_hydration_functions.get(value.tag)
        try:
            if not f:
                raise ValueError(
                    f"Protocol error: unknown Structure tag: {value.tag!r}"
                )
            return f(*value.fields)
        except Exception as e:
            return BrokenHydrationObject(e, value)

    @staticmethod
    def _hydrate_list(value):
        for v in value:
            if isinstance(v, BrokenHydrationObject):
                return BrokenHydrationObject(v.error, value)
        return value

    @staticmethod
    def _hydrate_dict(value):
        for v in value.values():
            if isinstance(v, BrokenHydrationObject):
                return BrokenHydrationObject(v.error, value)
        return value

    def get_graph(self):
        return self._graph_hydrator.graph
