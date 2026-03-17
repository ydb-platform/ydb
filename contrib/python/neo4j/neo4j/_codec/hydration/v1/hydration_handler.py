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


from datetime import (
    date,
    datetime,
    time,
    timedelta,
)

from ...._optional_deps import (
    np,
    pd,
)
from ....graph import (
    Graph,
    Node,
    Path,
)
from ....spatial import (
    CartesianPoint,
    Point,
    WGS84Point,
)
from ....time import (
    Date,
    DateTime,
    Duration,
    Time,
)
from ....vector import Vector
from .._common import (
    GraphHydrator,
    HydrationScope,
)
from .._interface import HydrationHandlerABC
from . import (
    spatial,
    temporal,
    vector,
)


class _GraphHydrator(GraphHydrator):
    def __init__(self):
        super().__init__()
        self.struct_hydration_functions = {
            **self.struct_hydration_functions,
            b"N": self.hydrate_node,
            b"R": self.hydrate_relationship,
            b"r": self.hydrate_unbound_relationship,
            b"P": self.hydrate_path,
        }

    def hydrate_node(self, id_, labels=None, properties=None, element_id=None):
        assert isinstance(self.graph, Graph)
        # backwards compatibility with Neo4j < 5.0
        if element_id is None:
            element_id = str(id_)

        try:
            inst = self.graph._nodes[element_id]
        except KeyError:
            inst = Node(self.graph, element_id, id_, labels, properties)
            self.graph._nodes[element_id] = inst
        else:
            # If we have already hydrated this node as the endpoint of
            # a relationship, it won't have any labels or properties.
            # Therefore, we need to add the ones we have here.
            if labels:
                inst._labels = inst._labels.union(labels)  # frozen_set
            if properties:
                inst._properties.update(properties)
        return inst

    def hydrate_relationship(
        self,
        id_,
        n0_id,
        n1_id,
        type_,
        properties=None,
        element_id=None,
        n0_element_id=None,
        n1_element_id=None,
    ):
        # backwards compatibility with Neo4j < 5.0
        if element_id is None:
            element_id = str(id_)
        if n0_element_id is None:
            n0_element_id = str(n0_id)
        if n1_element_id is None:
            n1_element_id = str(n1_id)

        inst = self.hydrate_unbound_relationship(
            id_, type_, properties, element_id
        )
        inst._start_node = self.hydrate_node(n0_id, element_id=n0_element_id)
        inst._end_node = self.hydrate_node(n1_id, element_id=n1_element_id)
        return inst

    def hydrate_unbound_relationship(
        self, id_, type_, properties=None, element_id=None
    ):
        assert isinstance(self.graph, Graph)
        # backwards compatibility with Neo4j < 5.0
        if element_id is None:
            element_id = str(id_)

        try:
            inst = self.graph._relationships[element_id]
        except KeyError:
            r = self.graph.relationship_type(type_)
            inst = r(self.graph, element_id, id_, properties)
            self.graph._relationships[element_id] = inst
        return inst

    def hydrate_path(self, nodes, relationships, sequence):
        assert isinstance(self.graph, Graph)
        assert len(nodes) >= 1
        assert len(sequence) % 2 == 0
        last_node = nodes[0]
        entities = [last_node]
        for i, rel_index in enumerate(sequence[::2]):
            assert rel_index != 0
            next_node = nodes[sequence[2 * i + 1]]
            if rel_index > 0:
                r = relationships[rel_index - 1]
                r._start_node = last_node
                r._end_node = next_node
                entities.append(r)
            else:
                r = relationships[-rel_index - 1]
                r._start_node = next_node
                r._end_node = last_node
                entities.append(r)
            last_node = next_node
        return Path(*entities)


class HydrationHandler(HydrationHandlerABC):
    def __init__(self):
        super().__init__()
        self.struct_hydration_functions = {
            **self.struct_hydration_functions,
            b"X": spatial.hydrate_point,
            b"Y": spatial.hydrate_point,
            b"D": temporal.hydrate_date,
            b"T": temporal.hydrate_time,  # time zone offset
            b"t": temporal.hydrate_time,  # no time zone
            b"F": temporal.hydrate_datetime,  # time zone offset
            b"f": temporal.hydrate_datetime,  # time zone name
            b"d": temporal.hydrate_datetime,  # no time zone
            b"E": temporal.hydrate_duration,
        }
        self.dehydration_hooks.update(
            exact_types={
                Point: spatial.dehydrate_point,
                CartesianPoint: spatial.dehydrate_point,
                WGS84Point: spatial.dehydrate_point,
                Date: temporal.dehydrate_date,
                date: temporal.dehydrate_date,
                Time: temporal.dehydrate_time,
                time: temporal.dehydrate_time,
                DateTime: temporal.dehydrate_datetime,
                datetime: temporal.dehydrate_datetime,
                Duration: temporal.dehydrate_duration,
                timedelta: temporal.dehydrate_timedelta,
                Vector: vector.dehydrate_vector,
            }
        )
        if np is not None:
            self.dehydration_hooks.update(
                exact_types={
                    np.datetime64: temporal.dehydrate_np_datetime,
                    np.timedelta64: temporal.dehydrate_np_timedelta,
                }
            )
        if pd is not None:
            self.dehydration_hooks.update(
                exact_types={
                    pd.Timestamp: temporal.dehydrate_pandas_datetime,
                    pd.Timedelta: temporal.dehydrate_pandas_timedelta,
                    type(pd.NaT): lambda _: None,
                }
            )

    def patch_utc(self):
        from ..v2 import temporal as temporal_v2

        del self.struct_hydration_functions[b"F"]
        del self.struct_hydration_functions[b"f"]
        self.struct_hydration_functions.update(
            {
                b"I": temporal_v2.hydrate_datetime,
                b"i": temporal_v2.hydrate_datetime,
            }
        )

        self.dehydration_hooks.update(
            exact_types={
                DateTime: temporal_v2.dehydrate_datetime,
                datetime: temporal_v2.dehydrate_datetime,
            }
        )
        if pd is not None:
            self.dehydration_hooks.update(
                exact_types={
                    pd.Timestamp: temporal_v2.dehydrate_pandas_datetime,
                }
            )

    def new_hydration_scope(self):
        return HydrationScope(self, _GraphHydrator())
