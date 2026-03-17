#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import contextlib
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    cast,
)

from typing_extensions import Self

from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import scan

from ..connections import get_connection
from ..response import Response
from ..search_base import MultiSearchBase, SearchBase
from ..utils import _R, AttrDict, UsingType


class Search(SearchBase[_R]):
    _using: UsingType

    def __iter__(self) -> Iterator[_R]:
        """
        Iterate over the hits.
        """

        class ResultsIterator(Iterator[_R]):
            def __init__(self, search: Search[_R]):
                self.search = search
                self.iterator: Optional[Iterator[_R]] = None

            def __next__(self) -> _R:
                if self.iterator is None:
                    self.iterator = iter(self.search.execute())
                try:
                    return next(self.iterator)
                except StopIteration:
                    raise StopIteration()

        return ResultsIterator(self)

    def count(self) -> int:
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        if hasattr(self, "_response") and self._response.hits.total.relation == "eq":  # type: ignore[attr-defined]
            return cast(int, self._response.hits.total.value)  # type: ignore[attr-defined]

        es = get_connection(self._using)

        d = self.to_dict(count=True)
        # TODO: failed shards detection
        resp = es.count(
            index=self._index,
            query=cast(Optional[Dict[str, Any]], d.get("query", None)),
            **self._params,
        )

        return cast(int, resp["count"])

    def execute(self, ignore_cache: bool = False) -> Response[_R]:
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.

        :arg ignore_cache: if set to ``True``, consecutive calls will hit
            ES, while cached result will be ignored. Defaults to `False`
        """
        if ignore_cache or not hasattr(self, "_response"):
            es = get_connection(self._using)

            self._response = self._response_class(
                self,
                (
                    es.search(index=self._index, body=self.to_dict(), **self._params)
                ).body,
            )
        return self._response

    def scan(self) -> Iterator[_R]:
        """
        Turn the search into a scan search and return a generator that will
        iterate over all the documents matching the query.

        Use the ``params`` method to specify any additional arguments you wish to
        pass to the underlying ``scan`` helper from ``elasticsearch-py`` -
        https://elasticsearch-py.readthedocs.io/en/latest/helpers.html#scan

        The ``iterate()`` method should be preferred, as it provides similar
        functionality using an Elasticsearch point in time.
        """
        es = get_connection(self._using)

        for hit in scan(es, query=self.to_dict(), index=self._index, **self._params):
            yield self._get_result(cast(AttrDict[Any], hit))

    def delete(self) -> AttrDict[Any]:
        """
        ``delete()`` executes the query by delegating to ``delete_by_query()``.

        Use the ``params`` method to specify any additional arguments you wish to
        pass to the underlying ``delete_by_query`` helper from ``elasticsearch-py`` -
        https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch.delete_by_query
        """

        es = get_connection(self._using)
        assert self._index is not None

        return AttrDict(
            cast(
                Dict[str, Any],
                es.delete_by_query(
                    index=self._index, body=self.to_dict(), **self._params
                ),
            )
        )

    @contextlib.contextmanager
    def point_in_time(self, keep_alive: str = "1m") -> Iterator[Self]:
        """
        Open a point in time (pit) that can be used across several searches.

        This method implements a context manager that returns a search object
        configured to operate within the created pit.

        :arg keep_alive: the time to live for the point in time, renewed with each search request
        """
        es = get_connection(self._using)

        pit = es.open_point_in_time(index=self._index or "*", keep_alive=keep_alive)
        search = self.index().extra(pit={"id": pit["id"], "keep_alive": keep_alive})
        if not search._sort:
            search = search.sort("_shard_doc")
        yield search
        es.close_point_in_time(id=pit["id"])

    def iterate(self, keep_alive: str = "1m") -> Iterator[_R]:
        """
        Return a generator that iterates over all the documents matching the query.

        This method uses a point in time to provide consistent results even when
        the index is changing. It should be preferred over ``scan()``.

        :arg keep_alive: the time to live for the point in time, renewed with each new search request
        """
        with self.point_in_time(keep_alive=keep_alive) as s:
            while True:
                r = s.execute()
                for hit in r:
                    yield hit
                if len(r.hits) == 0:
                    break
                s = s.search_after()


class MultiSearch(MultiSearchBase[_R]):
    """
    Combine multiple :class:`~elasticsearch.dsl.Search` objects into a single
    request.
    """

    _using: UsingType

    if TYPE_CHECKING:

        def add(self, search: Search[_R]) -> Self: ...  # type: ignore[override]

    def execute(
        self, ignore_cache: bool = False, raise_on_error: bool = True
    ) -> List[Response[_R]]:
        """
        Execute the multi search request and return a list of search results.
        """
        if ignore_cache or not hasattr(self, "_response"):
            es = get_connection(self._using)

            responses = es.msearch(
                index=self._index, body=self.to_dict(), **self._params
            )

            out: List[Response[_R]] = []
            for s, r in zip(self._searches, responses["responses"]):
                if r.get("error", False):
                    if raise_on_error:
                        raise ApiError("N/A", meta=responses.meta, body=r)
                    r = None
                else:
                    r = Response(s, r)
                out.append(r)

            self._response = out

        return self._response


class EmptySearch(Search[_R]):
    def count(self) -> int:
        return 0

    def execute(self, ignore_cache: bool = False) -> Response[_R]:
        return self._response_class(self, {"hits": {"total": 0, "hits": []}})

    def scan(self) -> Iterator[_R]:
        return
        yield  # a bit strange, but this forces an empty generator function

    def delete(self) -> AttrDict[Any]:
        return AttrDict[Any]({})
