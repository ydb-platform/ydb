# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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

import collections.abc as collections_abc
import copy
from typing import Any

from opensearchpy.connection.connections import get_connection
from opensearchpy.exceptions import TransportError
from opensearchpy.helpers import scan

from ..exceptions import IllegalOperation
from ..helpers.query import Bool, Q
from .aggs import A, AggBase
from .response import Hit, Response
from .utils import AttrDict, DslBase, recursive_to_dict


class QueryProxy:
    """
    Simple proxy around DSL objects (queries) that can be called
    (to add query/post_filter) and also allows attribute access which is proxied to
    the wrapped query.
    """

    def __init__(self, search: Any, attr_name: Any) -> None:
        self._search = search
        self._proxied: Any = None
        self._attr_name = attr_name

    def __nonzero__(self) -> bool:
        return self._proxied is not None

    __bool__ = __nonzero__

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        s = self._search._clone()

        # we cannot use self._proxied since we just cloned self._search and
        # need to access the new self on the clone
        proxied = getattr(s, self._attr_name)
        if proxied._proxied is None:
            proxied._proxied = Q(*args, **kwargs)
        else:
            proxied._proxied &= Q(*args, **kwargs)

        # always return search to be chainable
        return s

    def __getattr__(self, attr_name: Any) -> Any:
        return getattr(self._proxied, attr_name)

    def __setattr__(self, attr_name: Any, value: Any) -> None:
        if not attr_name.startswith("_"):
            self._proxied = Q(self._proxied.to_dict())
            setattr(self._proxied, attr_name, value)
        super().__setattr__(attr_name, value)

    def __getstate__(self) -> Any:
        return self._search, self._proxied, self._attr_name

    def __setstate__(self, state: Any) -> None:
        self._search, self._proxied, self._attr_name = state


class ProxyDescriptor:
    """
    Simple descriptor to enable setting of queries and filters as:

        s = Search()
        s.query = Q(...)

    """

    def __init__(self, name: str) -> None:
        self._attr_name = f"_{name}_proxy"

    def __get__(self, instance: Any, owner: Any) -> Any:
        return getattr(instance, self._attr_name)

    def __set__(self, instance: Any, value: Any) -> None:
        proxy = getattr(instance, self._attr_name)
        proxy._proxied = Q(value)


class AggsProxy(AggBase, DslBase):
    name = "aggs"

    def __init__(self, search: Any) -> None:
        self._base = self
        self._search = search
        self._params = {"aggs": {}}

    def to_dict(self) -> Any:
        return super().to_dict().get("aggs", {})


class Request:
    _doc_type: Any
    _doc_type_map: Any

    def __init__(
        self,
        using: str = "default",
        index: Any = None,
        doc_type: Any = None,
        extra: Any = None,
    ) -> None:
        self._using = using

        self._index = None
        if isinstance(index, (tuple, list)):
            self._index = list(index)
        elif index:
            self._index = [index]

        self._doc_type = []
        self._doc_type_map = {}
        if isinstance(doc_type, (tuple, list)):
            self._doc_type.extend(doc_type)
        elif isinstance(doc_type, collections_abc.Mapping):
            self._doc_type.extend(doc_type.keys())
            self._doc_type_map.update(doc_type)
        elif doc_type:
            self._doc_type.append(doc_type)

        self._params: Any = {}
        self._extra: Any = extra or {}

    def __eq__(self: Any, other: Any) -> bool:
        return (
            isinstance(other, Request)
            and other._params == self._params
            and other._index == self._index
            and other._doc_type == self._doc_type
            and other.to_dict() == self.to_dict()  # type: ignore
        )

    def __copy__(self) -> Any:
        return self._clone()

    def params(self, **kwargs: Any) -> Any:
        """
        Specify query params to be used when executing the search. All the
        keyword arguments will override the current values.

        Example::

            s = Search()
            s = s.params(routing='user-1', preference='local')
        """
        s = self._clone()
        s._params.update(kwargs)
        return s

    def index(self, *index: Any) -> Any:
        """
        Set the index for the search. If called empty it will remove all information.

        Example:

            s = Search()
            s = s.index('twitter-2015.01.01', 'twitter-2015.01.02')
            s = s.index(['twitter-2015.01.01', 'twitter-2015.01.02'])
        """
        # .index() resets
        s = self._clone()
        if not index:
            s._index = None
        else:
            indexes = []
            for i in index:
                if isinstance(i, str):
                    indexes.append(i)
                elif isinstance(i, list):
                    indexes += i
                elif isinstance(i, tuple):
                    indexes += list(i)

            s._index = (self._index or []) + indexes

        return s

    def _resolve_field(self, path: Any) -> Any:
        for dt in self._doc_type:
            if not hasattr(dt, "_index"):
                continue
            field = dt._index.resolve_field(path)
            if field is not None:
                return field

    def _resolve_nested(self, hit: Any, parent_class: Any = None) -> Any:
        doc_class = Hit

        nested_path: Any = []
        nesting = hit["_nested"]
        while nesting and "field" in nesting:
            nested_path.append(nesting["field"])
            nesting = nesting.get("_nested")
        nested_path = ".".join(nested_path)

        if hasattr(parent_class, "_index"):
            nested_field = parent_class._index.resolve_field(nested_path)
        else:
            nested_field = self._resolve_field(nested_path)

        if nested_field is not None:
            return nested_field._doc_class

        return doc_class

    def _get_result(self, hit: Any, parent_class: Any = None) -> Any:
        doc_class = Hit
        dt = hit.get("_type")

        if "_nested" in hit:
            doc_class = self._resolve_nested(hit, parent_class)

        elif dt in self._doc_type_map:
            doc_class = self._doc_type_map[dt]

        else:
            for doc_type in self._doc_type:
                if hasattr(doc_type, "_matches") and doc_type._matches(hit):
                    doc_class = doc_type
                    break

        for t in hit.get("inner_hits", ()):
            hit["inner_hits"][t] = Response(
                self, hit["inner_hits"][t], doc_class=doc_class
            )

        callback = getattr(doc_class, "from_opensearch", doc_class)
        return callback(hit)

    def doc_type(self, *doc_type: Any, **kwargs: Any) -> Any:
        """
        Set the type to search through. You can supply a single value or
        multiple. Values can be strings or subclasses of ``Document``.

        You can also pass in any keyword arguments, mapping a doc_type to a
        callback that should be used instead of the Hit class.

        If no doc_type is supplied any information stored on the instance will
        be erased.

        Example:

            s = Search().doc_type('product', 'store', User, custom=my_callback)
        """
        # .doc_type() resets
        s = self._clone()
        if not doc_type and not kwargs:
            s._doc_type = []
            s._doc_type_map = {}
        else:
            s._doc_type.extend(doc_type)
            s._doc_type.extend(kwargs.keys())
            s._doc_type_map.update(kwargs)
        return s

    def using(self, client: Any) -> Any:
        """
        Associate the search request with an opensearch client. A fresh copy
        will be returned with current instance remaining unchanged.

        :arg client: an instance of ``opensearchpy.OpenSearch`` to use or
            an alias to look up in ``opensearchpy.connections``

        """
        s = self._clone()
        s._using = client
        return s

    def extra(self, **kwargs: Any) -> Any:
        """
        Add extra keys to the request body. Mostly here for backwards
        compatibility.
        """
        s = self._clone()
        if "from_" in kwargs:
            kwargs["from"] = kwargs.pop("from_")
        s._extra.update(kwargs)
        return s

    def _clone(self) -> Any:
        s = self.__class__(
            using=self._using, index=self._index, doc_type=self._doc_type
        )
        s._doc_type_map = self._doc_type_map.copy()
        s._extra = self._extra.copy()
        s._params = self._params.copy()
        return s


class Search(Request):
    query = ProxyDescriptor("query")
    post_filter = ProxyDescriptor("post_filter")

    def __init__(self, **kwargs: Any) -> None:
        """
        Search request to opensearch.

        :arg using: `OpenSearch` instance to use
        :arg index: limit the search to index
        :arg doc_type: only query this type.

        All the parameters supplied (or omitted) at creation type can be later
        overridden by methods (`using`, `index` and `doc_type` respectively).
        """
        super().__init__(**kwargs)

        self.aggs = AggsProxy(self)
        self._sort: Any = []
        self._collapse: Any = {}
        self._source: Any = None
        self._highlight: Any = {}
        self._highlight_opts: Any = {}
        self._suggest: Any = {}
        self._script_fields: Any = {}
        self._response_class = Response

        self._query_proxy = QueryProxy(self, "query")
        self._post_filter_proxy = QueryProxy(self, "post_filter")

    def filter(self, *args: Any, **kwargs: Any) -> Any:
        return self.query(Bool(filter=[Q(*args, **kwargs)]))

    def exclude(self, *args: Any, **kwargs: Any) -> Any:
        return self.query(Bool(filter=[~Q(*args, **kwargs)]))

    def __iter__(self) -> Any:
        """
        Iterate over the hits.
        """
        return iter(self.execute())

    def __getitem__(self, n: Any) -> Any:
        """
        Support slicing the `Search` instance for pagination.

        Slicing equates to the from/size parameters. E.g.::

            s = Search().query(...)[0:25]

        is equivalent to::

            s = Search().query(...).extra(from_=0, size=25)

        """
        s = self._clone()

        if isinstance(n, slice):
            # If negative slicing, abort.
            if n.start and n.start < 0 or n.stop and n.stop < 0:
                raise ValueError("Search does not support negative slicing.")
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
                raise ValueError("Search does not support negative indexing.")
            s._extra["from"] = n
            s._extra["size"] = 1
            return s

    @classmethod
    def from_dict(cls, d: Any) -> Any:
        """
        Construct a new `Search` instance from a raw dict containing the search
        body. Useful when migrating from raw dictionaries.

        Example::

            s = Search.from_dict({
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

    def _clone(self) -> Any:
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """
        s = super()._clone()

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

    def update_from_dict(self, d: Any) -> "Search":
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

            s = Search()
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

            s = Search()
            s = s.source(includes=['obj1.*'], excludes=["*.description"])

            s = Search()
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

            s = Search().sort(
                'category',
                '-title',
                {"price" : {"order" : "asc", "mode" : "avg"}}
            )

        will sort by ``category``, ``title`` (in descending order) and
        ``price`` in ascending order using the ``avg`` mode.

        The API returns a copy of the Search object and can thus be chained.
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
    ) -> Any:
        """
        Add collapsing information to the search request.

        If called without providing ``field``, it will remove all collapse
        requirements, otherwise it will replace them with the provided
        arguments.

        The API returns a copy of the Search object and can thus be chained.
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

            s = Search()
            s = s.highlight_options(order='score')
        """
        s = self._clone()
        s._highlight_opts.update(kwargs)
        return s

    def highlight(self, *fields: Any, **kwargs: Any) -> Any:
        """
        Request highlighting of some fields. All keyword arguments passed in will be
        used as parameters for all the fields in the ``fields`` parameter. Example::

            Search().highlight('title', 'body', fragment_size=50)

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

            Search().highlight('title', fragment_size=50).highlight('body', fragment_size=100)

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

    def suggest(self, name: Any, text: Any, **kwargs: Any) -> Any:
        """
        Add a suggestions request to the search.

        :arg name: name of the suggestion
        :arg text: text to suggest on

        All keyword arguments will be added to the suggestions body. For example::

            s = Search()
            s = s.suggest('suggestion-1', 'OpenSearch', term={'field': 'body'})
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

    def count(self) -> Any:
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        if hasattr(self, "_response") and self._response.hits.total.relation == "eq":
            return self._response.hits.total.value

        opensearch = get_connection(self._using)

        d = self.to_dict(count=True)
        # TODO: failed shards detection
        return opensearch.count(index=self._index, body=d, **self._params)["count"]

    def execute(self, ignore_cache: bool = False) -> Any:
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.

        :arg ignore_cache: if set to ``True``, consecutive calls will hit
            OpenSearch, while cached result will be ignored. Defaults to `False`
        """
        if ignore_cache or not hasattr(self, "_response"):
            opensearch = get_connection(self._using)

            self._response = self._response_class(
                self,
                opensearch.search(
                    index=self._index, body=self.to_dict(), **self._params
                ),
            )
        return self._response

    def scan(self) -> Any:
        """
        Turn the search into a scan search and return a generator that will
        iterate over all the documents matching the query.

        Use ``params`` method to specify any additional arguments you with to
        pass to the underlying ``scan`` helper from ``opensearchpy``

        """
        opensearch = get_connection(self._using)

        for hit in scan(
            opensearch, query=self.to_dict(), index=self._index, **self._params
        ):
            yield self._get_result(hit)

    def delete(self) -> Any:
        """
        delete() executes the query by delegating to delete_by_query()
        """

        opensearch = get_connection(self._using)

        return AttrDict(
            opensearch.delete_by_query(
                index=self._index, body=self.to_dict(), **self._params
            )
        )


class MultiSearch(Request):
    """
    Combine multiple :class:`~opensearchpy.Search` objects into a single
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
        Adds a new :class:`~opensearchpy.Search` object to the request::

            ms = MultiSearch(index='my-index')
            ms = ms.add(Search(doc_type=Category).filter('term', category='python'))
            ms = ms.add(Search(doc_type=Blog))
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

    def execute(self, ignore_cache: Any = False, raise_on_error: Any = True) -> Any:
        """
        Execute the multi search request and return a list of search results.
        """
        if ignore_cache or not hasattr(self, "_response"):
            opensearch = get_connection(self._using)

            responses = opensearch.msearch(
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


__all__ = ["Q"]
