# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import copy
from typing import Any, Dict, Sequence, cast

from opensearchpy._async.helpers.actions import aiter, async_scan
from opensearchpy.connection.async_connections import get_connection
from opensearchpy.exceptions import IllegalOperation, TransportError
from opensearchpy.helpers.aggs import A
from opensearchpy.helpers.query import Bool, Q
from opensearchpy.helpers.response import Response
from opensearchpy.helpers.search import AggsProxy, ProxyDescriptor, QueryProxy, Request
from opensearchpy.helpers.utils import AttrDict, recursive_to_dict


class AsyncSearch(Request):
    query = ProxyDescriptor("query")
    post_filter = ProxyDescriptor("post_filter")

    def __init__(self, **kwargs: Any) -> None:
        """
        Search request to opensearch.

        :arg using: `AsyncOpenSearch` instance to use
        :arg index: limit the search to index
        :arg doc_type: only query this type.

        All the parameters supplied (or omitted) at creation type can be later
        overridden by methods (`using`, `index` and `doc_type` respectively).
        """
        super().__init__(**kwargs)

        self.aggs = AggsProxy(self)
        self._sort: Sequence[Any] = []
        self._collapse: Dict[str, Any] = {}
        self._source: Any = None
        self._highlight: Any = {}
        self._highlight_opts: Any = {}
        self._suggest: Any = {}
        self._script_fields: Any = {}
        self._response_class: Any = Response

        self._query_proxy = QueryProxy(self, "query")
        self._post_filter_proxy = QueryProxy(self, "post_filter")

    def filter(self, *args: Any, **kwargs: Any) -> Any:
        return self.query(Bool(filter=[Q(*args, **kwargs)]))

    def exclude(self, *args: Any, **kwargs: Any) -> Any:
        return self.query(Bool(filter=[~Q(*args, **kwargs)]))

    def __getitem__(self, n: Any) -> Any:
        """
        Support slicing the `AsyncSearch` instance for pagination.

        Slicing equates to the from/size parameters. E.g.::

            s = AsyncSearch().query(...)[0:25]

        is equivalent to::

            s = AsyncSearch().query(...).extra(from_=0, size=25)

        """
        s = self._clone()

        if isinstance(n, slice):
            # If negative slicing, abort.
            if n.start and n.start < 0 or n.stop and n.stop < 0:
                raise ValueError("AsyncSearch does not support negative slicing.")
            # OpenSearch won't get all results so we default to size: 10 if
            # stop not given.
            s._extra["from"] = n.start or 0
            s._extra["size"] = max(
                0, n.stop - (n.start or 0) if n.stop is not None else 10
            )
            return s
        else:  # This is an index lookup, equivalent to slicing by [n:n+1].
            # If negative index, abort.
            if n < 0:
                raise ValueError("AsyncSearch does not support negative indexing.")
            s._extra["from"] = n
            s._extra["size"] = 1
            return s

    @classmethod
    def from_dict(cls, d: Any) -> Any:
        """
        Construct a new `AsyncSearch` instance from a raw dict containing the search
        body. Useful when migrating from raw dictionaries.

        Example::

            s = AsyncSearch.from_dict({
                "query": {
                    "bool": {
                        "must": [...]
                    }
                },
                "aggs": {...}
            })
            s = s.filter('term', published=True)
        """
        s = cls()
        s.update_from_dict(d)
        return s

    def _clone(self) -> "AsyncSearch":
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """
        s = cast(AsyncSearch, super()._clone())

        s._response_class = self._response_class
        s._sort = self._sort[:]
        s._source = copy.copy(self._source) if self._source is not None else None
        s._highlight = self._highlight.copy()
        s._highlight_opts = self._highlight_opts.copy()
        s._suggest = self._suggest.copy()
        s._script_fields = self._script_fields.copy()
        s._collapse = self._collapse.copy()
        for x in ("query", "post_filter"):
            getattr(s, x)._proxied = getattr(self, x)._proxied

        # copy top-level bucket definitions
        if self.aggs._params.get("aggs"):
            s.aggs._params = {"aggs": self.aggs._params["aggs"].copy()}
        return s

    def response_class(self, cls: Any) -> Any:
        """
        Override the default wrapper used for the response.
        """
        s = self._clone()
        s._response_class = cls
        return s

    def update_from_dict(self, d: Any) -> "AsyncSearch":
        """
        Apply options from a serialized body to the current instance. Modifies
        the object in-place. Used mostly by ``from_dict``.
        """
        d = d.copy()
        if "query" in d:
            self.query._proxied = Q(d.pop("query"))
        if "post_filter" in d:
            self.post_filter._proxied = Q(d.pop("post_filter"))

        aggs = d.pop("aggs", d.pop("aggregations", {}))
        if aggs:
            self.aggs._params = {
                "aggs": {name: A(value) for (name, value) in aggs.items()}
            }
        if "sort" in d:
            self._sort = d.pop("sort")
        if "_source" in d:
            self._source = d.pop("_source")
        if "highlight" in d:
            high = d.pop("highlight").copy()
            self._highlight = high.pop("fields")
            self._highlight_opts = high
        if "suggest" in d:
            self._suggest = d.pop("suggest")
            if "text" in self._suggest:
                text = self._suggest.pop("text")
                for s in self._suggest.values():
                    s.setdefault("text", text)
        if "script_fields" in d:
            self._script_fields = d.pop("script_fields")
        self._extra.update(d)
        return self

    def script_fields(self, **kwargs: Any) -> Any:
        """
        Define script fields to be calculated on hits.

        Example::

            s = AsyncSearch()
            s = s.script_fields(times_two="doc['field'].value * 2")
            s = s.script_fields(
                times_three={
                    'script': {
                        'lang': 'painless',
                        'source': "doc['field'].value * params.n",
                        'params': {'n': 3}
                    }
                }
            )

        """
        s = self._clone()
        for name in kwargs:
            if isinstance(kwargs[name], str):
                kwargs[name] = {"script": kwargs[name]}
        s._script_fields.update(kwargs)
        return s

    def source(self, fields: Any = None, **kwargs: Any) -> Any:
        """
        Selectively control how the _source field is returned.

        :arg fields: wildcard string, array of wildcards, or dictionary of includes and excludes

        If ``fields`` is None, the entire document will be returned for
        each hit.  If fields is a dictionary with keys of 'includes' and/or
        'excludes' the fields will be either included or excluded appropriately.

        Calling this multiple times with the same named parameter will override the
        previous values with the new ones.

        Example::

            s = AsyncSearch()
            s = s.source(includes=['obj1.*'], excludes=["*.description"])

            s = AsyncSearch()
            s = s.source(includes=['obj1.*']).source(excludes=["*.description"])

        """
        s = self._clone()

        if fields and kwargs:
            raise ValueError("You cannot specify fields and kwargs at the same time.")

        if fields is not None:
            s._source = fields
            return s

        if kwargs and not isinstance(s._source, dict):
            s._source = {}

        for key, value in kwargs.items():
            if value is None:
                try:
                    del s._source[key]
                except KeyError:
                    pass
            else:
                s._source[key] = value

        return s

    def sort(self, *keys: Any) -> Any:
        """
        Add sorting information to the search request. If called without
        arguments it will remove all sort requirements. Otherwise it will
        replace them. Acceptable arguments are::

            'some.field'
            '-some.other.field'
            {'different.field': {'any': 'dict'}}

        so for example::

            s = AsyncSearch().sort(
                'category',
                '-title',
                {"price" : {"order" : "asc", "mode" : "avg"}}
            )

        will sort by ``category``, ``title`` (in descending order) and
        ``price`` in ascending order using the ``avg`` mode.

        The API returns a copy of the AsyncSearch object and can thus be chained.
        """
        s = self._clone()
        s._sort = []
        for k in keys:
            if isinstance(k, str) and k.startswith("-"):
                if k[1:] == "_score":
                    raise IllegalOperation("Sorting by `-_score` is not allowed.")
                k = {k[1:]: {"order": "desc"}}
            s._sort.append(k)
        return s

    def collapse(
        self,
        field: Any = None,
        inner_hits: Any = None,
        max_concurrent_group_searches: Any = None,
    ) -> "AsyncSearch":
        """
        Add collapsing information to the search request.

        If called without providing ``field``, it will remove all collapse
        requirements, otherwise it will replace them with the provided
        arguments.

        The API returns a copy of the AsyncSearch object and can thus be chained.
        """
        s = self._clone()
        s._collapse = {}

        if field is None:
            return s

        s._collapse["field"] = field
        if inner_hits:
            s._collapse["inner_hits"] = inner_hits
        if max_concurrent_group_searches:
            s._collapse["max_concurrent_group_searches"] = max_concurrent_group_searches
        return s

    def highlight_options(self, **kwargs: Any) -> Any:
        """
        Update the global highlighting options used for this request. For
        example::

            s = AsyncSearch()
            s = s.highlight_options(order='score')
        """
        s = self._clone()
        s._highlight_opts.update(kwargs)
        return s

    def highlight(self, *fields: Any, **kwargs: Any) -> Any:
        """
        Request highlighting of some fields. All keyword arguments passed in will be
        used as parameters for all the fields in the ``fields`` parameter. Example::

            AsyncSearch().highlight('title', 'body', fragment_size=50)

        will produce the equivalent of::

            {
                "highlight": {
                    "fields": {
                        "body": {"fragment_size": 50},
                        "title": {"fragment_size": 50}
                    }
                }
            }

        If you want to have different options for different fields
        you can call ``highlight`` twice::

            AsyncSearch().highlight('title', fragment_size=50).highlight('body', fragment_size=100)

        which will produce::

            {
                "highlight": {
                    "fields": {
                        "body": {"fragment_size": 100},
                        "title": {"fragment_size": 50}
                    }
                }
            }

        """
        s = self._clone()
        for f in fields:
            s._highlight[f] = kwargs
        return s

    def suggest(self, name: str, text: str, **kwargs: Any) -> Any:
        """
        Add a suggestions request to the search.

        :arg name: name of the suggestion
        :arg text: text to suggest on

        All keyword arguments will be added to the suggestions body. For example::

            s = AsyncSearch()
            s = s.suggest('suggestion-1', 'AsyncOpenSearch', term={'field': 'body'})
        """
        s = self._clone()
        s._suggest[name] = {"text": text}
        s._suggest[name].update(kwargs)
        return s

    def to_dict(self, count: bool = False, **kwargs: Any) -> Any:
        """
        Serialize the search into the dictionary that will be sent over as the
        request's body.

        :arg count: a flag to specify if we are interested in a body for count -
            no aggregations, no pagination bounds etc.

        All additional keyword arguments will be included into the dictionary.
        """
        d = {}

        if self.query:
            d["query"] = self.query.to_dict()

        # count request doesn't care for sorting and other things
        if not count:
            if self.post_filter:
                d["post_filter"] = self.post_filter.to_dict()

            if self.aggs.aggs:
                d.update(self.aggs.to_dict())

            if self._sort:
                d["sort"] = self._sort

            if self._collapse:
                d["collapse"] = self._collapse

            d.update(recursive_to_dict(self._extra))

            if self._source not in (None, {}):
                d["_source"] = self._source

            if self._highlight:
                d["highlight"] = {"fields": self._highlight}
                d["highlight"].update(self._highlight_opts)

            if self._suggest:
                d["suggest"] = self._suggest

            if self._script_fields:
                d["script_fields"] = self._script_fields

        d.update(recursive_to_dict(kwargs))
        return d

    async def count(self) -> Any:
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        if hasattr(self, "_response") and self._response.hits.total.relation == "eq":
            return self._response.hits.total.value

        opensearch = await get_connection(self._using)

        d = self.to_dict(count=True)
        # TODO: failed shards detection
        return (await opensearch.count(index=self._index, body=d, **self._params))[
            "count"
        ]

    async def execute(self, ignore_cache: bool = False) -> Any:
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.

        :arg ignore_cache: if set to ``True``, consecutive calls will hit
            AsyncOpenSearch, while cached result will be ignored. Defaults to `False`
        """
        if ignore_cache or not hasattr(self, "_response"):
            opensearch = await get_connection(self._using)

            self._response = self._response_class(
                self,
                await opensearch.search(
                    index=self._index, body=self.to_dict(), **self._params
                ),
            )
        return self._response

    async def scan(self) -> Any:
        """
        Turn the search into a scan search and return a generator that will
        iterate over all the documents matching the query.

        Use ``params`` method to specify any additional arguments you with to
        pass to the underlying ``async_scan`` helper from ``opensearchpy``

        """
        opensearch = await get_connection(self._using)

        async for hit in aiter(
            async_scan(
                opensearch, query=self.to_dict(), index=self._index, **self._params
            )
        ):
            yield self._get_result(hit)

    async def delete(self) -> Any:
        """
        delete() executes the query by delegating to delete_by_query()
        """

        opensearch = await get_connection(self._using)

        return AttrDict(
            await opensearch.delete_by_query(
                index=self._index, body=self.to_dict(), **self._params
            )
        )


class AsyncMultiSearch(Request):
    """
    Combine multiple :class:`~opensearchpy.AsyncSearch` objects into a single
    request.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._searches: Any = []

    def __getitem__(self, key: Any) -> Any:
        return self._searches[key]

    def __iter__(self) -> Any:
        return iter(self._searches)

    def _clone(self) -> Any:
        ms = super()._clone()
        ms._searches = self._searches[:]
        return ms

    def add(self, search: Any) -> Any:
        """
        Adds a new :class:`~opensearchpy.AsyncSearch` object to the request::

            ms = AsyncMultiSearch(index='my-index')
            ms = ms.add(AsyncSearch(doc_type=Category).filter('term', category='python'))
            ms = ms.add(AsyncSearch(doc_type=Blog))
        """
        ms = self._clone()
        ms._searches.append(search)
        return ms

    def to_dict(self) -> Any:
        out = []
        for s in self._searches:
            meta = {}
            if s._index:
                meta["index"] = s._index
            meta.update(s._params)

            out.append(meta)
            out.append(s.to_dict())

        return out

    async def execute(
        self, ignore_cache: bool = False, raise_on_error: bool = True
    ) -> Any:
        """
        Execute the multi search request and return a list of search results.
        """
        if ignore_cache or not hasattr(self, "_response"):
            opensearch = await get_connection(self._using)

            responses = await opensearch.msearch(
                index=self._index, body=self.to_dict(), **self._params
            )

            out = []
            for s, r in zip(self._searches, responses["responses"]):
                if r.get("error", False):
                    if raise_on_error:
                        raise TransportError("N/A", r["error"]["type"], r["error"])
                    r = None
                else:
                    r = Response(s, r)
                out.append(r)

            self._response = out

        return self._response
