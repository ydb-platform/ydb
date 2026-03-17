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
from .._common import HydrationScope
from .._interface import HydrationHandlerABC
from ..v1 import (
    spatial,
    temporal as temporal_v1,
)
from ..v1.hydration_handler import _GraphHydrator
from ..v2 import temporal as temporal_v2
from . import (
    unsupported,
    vector,
)


class HydrationHandler(HydrationHandlerABC):  # type: ignore[no-redef]
    def __init__(self):
        super().__init__()
        self._created_scope = False
        self.struct_hydration_functions = {
            **self.struct_hydration_functions,
            b"X": spatial.hydrate_point,
            b"Y": spatial.hydrate_point,
            b"D": temporal_v1.hydrate_date,
            b"T": temporal_v1.hydrate_time,  # time zone offset
            b"t": temporal_v1.hydrate_time,  # no time zone
            b"I": temporal_v2.hydrate_datetime,  # time zone offset
            b"i": temporal_v2.hydrate_datetime,  # time zone name
            b"d": temporal_v2.hydrate_datetime,  # no time zone
            b"E": temporal_v1.hydrate_duration,
            b"V": vector.hydrate_vector,
            b"?": unsupported.hydrate_unsupported,
        }
        self.dehydration_hooks.update(
            exact_types={
                Point: spatial.dehydrate_point,
                CartesianPoint: spatial.dehydrate_point,
                WGS84Point: spatial.dehydrate_point,
                Date: temporal_v1.dehydrate_date,
                date: temporal_v1.dehydrate_date,
                Time: temporal_v1.dehydrate_time,
                time: temporal_v1.dehydrate_time,
                DateTime: temporal_v2.dehydrate_datetime,
                datetime: temporal_v2.dehydrate_datetime,
                Duration: temporal_v1.dehydrate_duration,
                timedelta: temporal_v1.dehydrate_timedelta,
                Vector: vector.dehydrate_vector,
            }
        )
        if np is not None:
            self.dehydration_hooks.update(
                exact_types={
                    np.datetime64: temporal_v1.dehydrate_np_datetime,
                    np.timedelta64: temporal_v1.dehydrate_np_timedelta,
                }
            )
        if pd is not None:
            self.dehydration_hooks.update(
                exact_types={
                    pd.Timestamp: temporal_v2.dehydrate_pandas_datetime,
                    pd.Timedelta: temporal_v1.dehydrate_pandas_timedelta,
                    type(pd.NaT): lambda _: None,
                }
            )

    def new_hydration_scope(self):
        self._created_scope = True
        return HydrationScope(self, _GraphHydrator())
