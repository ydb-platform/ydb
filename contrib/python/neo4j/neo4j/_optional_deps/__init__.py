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


from __future__ import annotations

from contextlib import suppress

from .. import _typing as t  # noqa: TC001


np: t.Any = None

with suppress(ImportError):
    import numpy as np  # type: ignore[no-redef]

pd: t.Any = None

with suppress(ImportError):
    import pandas as pd  # type: ignore[no-redef]

pa: t.Any = None

with suppress(ImportError):
    import pyarrow as pa  # type: ignore[no-redef]


__all__ = [
    "np",
    "pa",
    "pd",
]
