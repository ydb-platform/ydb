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

from .. import _typing as t


if t.TYPE_CHECKING:
    from .._data import Record
    from .summary import ResultSummary


class EagerResult(t.NamedTuple):
    """
    In-memory result of a query.

    It's a named tuple with 3 elements:
     * records - the list of records returned by the query
       (list of :class:`.Record` objects)
     * summary - the summary of the query execution
       (:class:`.ResultSummary` object)
     * keys - the list of keys returned by the query
       (see :attr:`AsyncResult.keys` and :attr:`.Result.keys`)

    .. seealso::
        :attr:`.AsyncDriver.execute_query`, :attr:`.Driver.execute_query`
            Which by default return an instance of this class.

        :attr:`.AsyncResult.to_eager_result`, :attr:`.Result.to_eager_result`
            Which can be used to convert to instance of this class.

    .. versionadded:: 5.5

    .. versionchanged:: 5.8 Stabilized from experimental.
    """

    #: Alias for field 0 (``eager_result[0]``)
    records: list[Record]
    #: Alias for field 1 (``eager_result[1]``)
    summary: ResultSummary
    #: Alias for field 2 (``eager_result[2]``)
    keys: list[str]
