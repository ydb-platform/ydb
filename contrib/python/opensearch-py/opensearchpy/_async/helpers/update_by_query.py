# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from typing import Any

from opensearchpy.connection.async_connections import get_connection
from opensearchpy.helpers.query import Bool, Q
from opensearchpy.helpers.response import UpdateByQueryResponse
from opensearchpy.helpers.search import ProxyDescriptor, QueryProxy, Request
from opensearchpy.helpers.utils import recursive_to_dict


class AsyncUpdateByQuery(Request):
    query = ProxyDescriptor("query")

    def __init__(self, **kwargs: Any) -> None:
        """
        Update by query request to opensearch.

        :arg using: `AsyncOpenSearch` instance to use
        :arg index: limit the search to index
        :arg doc_type: only query this type.

        All the parameters supplied (or omitted) at creation type can be later
        overridden by methods (`using`, `index` and `doc_type` respectively).

        """
        super().__init__(**kwargs)
        self._response_class = UpdateByQueryResponse
        self._script: Any = {}
        self._query_proxy = QueryProxy(self, "query")

    def filter(self, *args: Any, **kwargs: Any) -> Any:
        return self.query(Bool(filter=[Q(*args, **kwargs)]))

    def exclude(self, *args: Any, **kwargs: Any) -> Any:
        return self.query(Bool(filter=[~Q(*args, **kwargs)]))

    @classmethod
    def from_dict(cls, d: Any) -> Any:
        """
        Construct a new `AsyncUpdateByQuery` instance from a raw dict containing the search
        body. Useful when migrating from raw dictionaries.

        Example::

            ubq = AsyncUpdateByQuery.from_dict({
                "query": {
                    "bool": {
                        "must": [...]
                    }
                },
                "script": {...}
            })
            ubq = ubq.filter('term', published=True)
        """
        u = cls()
        u.update_from_dict(d)
        return u

    def _clone(self) -> Any:
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """
        ubq = super()._clone()

        ubq._response_class = self._response_class
        ubq._script = self._script.copy()
        ubq.query._proxied = self.query._proxied
        return ubq

    def response_class(self, cls: Any) -> Any:
        """
        Override the default wrapper used for the response.
        """
        ubq = self._clone()
        ubq._response_class = cls
        return ubq

    def update_from_dict(self, d: Any) -> "AsyncUpdateByQuery":
        """
        Apply options from a serialized body to the current instance. Modifies
        the object in-place. Used mostly by ``from_dict``.
        """
        d = d.copy()
        if "query" in d:
            self.query._proxied = Q(d.pop("query"))
        if "script" in d:
            self._script = d.pop("script")
        self._extra.update(d)
        return self

    def script(self, **kwargs: Any) -> Any:
        """
        Define update action to take:

        Note: the API only accepts a single script, so
        calling the script multiple times will overwrite.

        Example::

            ubq = AsyncSearch()
            ubq = ubq.script(source="ctx._source.likes++"")
            ubq = ubq.script(source="ctx._source.likes += params.f"",
                         lang="expression",
                         params={'f': 3})
        """
        ubq = self._clone()
        if ubq._script:
            ubq._script = {}
        ubq._script.update(kwargs)
        return ubq

    def to_dict(self, **kwargs: Any) -> Any:
        """
        Serialize the search into the dictionary that will be sent over as the
        request'ubq body.

        All additional keyword arguments will be included into the dictionary.
        """
        d = {}
        if self.query:
            d["query"] = self.query.to_dict()

        if self._script:
            d["script"] = self._script

        d.update(recursive_to_dict(self._extra))
        d.update(recursive_to_dict(kwargs))
        return d

    async def execute(self) -> Any:
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.
        """
        opensearch = await get_connection(self._using)

        self._response = self._response_class(
            self,
            await opensearch.update_by_query(
                index=self._index, body=self.to_dict(), **self._params
            ),
        )
        return self._response
