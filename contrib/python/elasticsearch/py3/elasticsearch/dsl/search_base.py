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

import collections.abc
import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import Self, TypeVar

from .aggs import A, Agg, AggBase
from .document_base import InstrumentedField
from .exceptions import IllegalOperation
from .query import Bool, Q, Query
from .response import Hit, Response
from .utils import _R, AnyUsingType, AttrDict, DslBase, recursive_to_dict

if TYPE_CHECKING:
    from .field import Field, Object


class SupportsClone(Protocol):
    def _clone(self) -> Self: ...


_S = TypeVar("_S", bound=SupportsClone)


class QueryProxy(Generic[_S]):
    """
    Simple proxy around DSL objects (queries) that can be called
    (to add query/post_filter) and also allows attribute access which is proxied to
    the wrapped query.
    """

    def __init__(self, search: _S, attr_name: str):
        self._search = search
        self._proxied: Optional[Query] = None
        self._attr_name = attr_name

    def __nonzero__(self) -> bool:
        return self._proxied is not None

    __bool__ = __nonzero__

    def __call__(self, *args: Any, **kwargs: Any) -> _S:
        """
        Add a query.
        """
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

    def __getattr__(self, attr_name: str) -> Any:
        return getattr(self._proxied, attr_name)

    def __setattr__(self, attr_name: str, value: Any) -> None:
        if not attr_name.startswith("_"):
            if self._proxied is not None:
                self._proxied = Q(self._proxied.to_dict())
                setattr(self._proxied, attr_name, value)
        super().__setattr__(attr_name, value)

    def __getstate__(self) -> Tuple[_S, Optional[Query], str]:
        return self._search, self._proxied, self._attr_name

    def __setstate__(self, state: Tuple[_S, Optional[Query], str]) -> None:
        self._search, self._proxied, self._attr_name = state


class ProxyDescriptor(Generic[_S]):
    """
    Simple descriptor to enable setting of queries and filters as:

        s = Search()
        s.query = Q(...)

    """

    def __init__(self, name: str):
        self._attr_name = f"_{name}_proxy"

    def __get__(self, instance: Any, owner: object) -> QueryProxy[_S]:
        return cast(QueryProxy[_S], getattr(instance, self._attr_name))

    def __set__(self, instance: _S, value: Dict[str, Any]) -> None:
        proxy: QueryProxy[_S] = getattr(instance, self._attr_name)
        proxy._proxied = Q(value)


class AggsProxy(AggBase[_R], DslBase):
    name = "aggs"

    def __init__(self, search: "SearchBase[_R]"):
        self._base = cast("Agg[_R]", self)
        self._search = search
        self._params = {"aggs": {}}

    def to_dict(self) -> Dict[str, Any]:
        return cast(Dict[str, Any], super().to_dict().get("aggs", {}))


class Request(Generic[_R]):
    def __init__(
        self,
        using: AnyUsingType = "default",
        index: Optional[Union[str, List[str]]] = None,
        doc_type: Optional[
            Union[type, str, List[Union[type, str]], Dict[str, Union[type, str]]]
        ] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        self._using = using

        self._index = None
        if isinstance(index, (tuple, list)):
            self._index = list(index)
        elif index:
            self._index = [index]

        self._doc_type: List[Union[type, str]] = []
        self._doc_type_map: Dict[str, Any] = {}
        if isinstance(doc_type, (tuple, list)):
            self._doc_type.extend(doc_type)
        elif isinstance(doc_type, collections.abc.Mapping):
            self._doc_type.extend(doc_type.keys())
            self._doc_type_map.update(doc_type)
        elif doc_type:
            self._doc_type.append(doc_type)

        self._params: Dict[str, Any] = {}
        self._extra: Dict[str, Any] = extra or {}

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Request)
            and other._params == self._params
            and other._index == self._index
            and other._doc_type == self._doc_type
            and other.to_dict() == self.to_dict()
        )

    def __copy__(self) -> Self:
        return self._clone()

    def params(self, **kwargs: Any) -> Self:
        """
        Specify query params to be used when executing the search. All the
        keyword arguments will override the current values. See
        https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch.search
        for all available parameters.

        Example::

            s = Search()
            s = s.params(routing='user-1', preference='local')
        """
        s = self._clone()
        s._params.update(kwargs)
        return s

    def index(self, *index: Union[str, List[str], Tuple[str, ...]]) -> Self:
        """
        Set the index for the search. If called empty it will remove all information.

        Example::

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

    def _resolve_field(self, path: str) -> Optional["Field"]:
        for dt in self._doc_type:
            if not hasattr(dt, "_index"):
                continue
            field = dt._index.resolve_field(path)
            if field is not None:
                return cast("Field", field)
        return None

    def _resolve_nested(
        self, hit: AttrDict[Any], parent_class: Optional[type] = None
    ) -> Type[_R]:
        doc_class = Hit

        nested_path = []
        nesting = hit["_nested"]
        while nesting and "field" in nesting:
            nested_path.append(nesting["field"])
            nesting = nesting.get("_nested")
        nested_path_str = ".".join(nested_path)

        nested_field: Optional["Object"]
        if parent_class is not None and hasattr(parent_class, "_index"):
            nested_field = cast(
                Optional["Object"], parent_class._index.resolve_field(nested_path_str)
            )
        else:
            nested_field = cast(
                Optional["Object"], self._resolve_field(nested_path_str)
            )

        if nested_field is not None:
            return cast(Type[_R], nested_field._doc_class)

        return cast(Type[_R], doc_class)

    def _get_result(
        self, hit: AttrDict[Any], parent_class: Optional[type] = None
    ) -> _R:
        doc_class: Any = Hit
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
            hit["inner_hits"][t] = Response[_R](
                self, hit["inner_hits"][t], doc_class=doc_class
            )

        callback = getattr(doc_class, "from_es", doc_class)
        return cast(_R, callback(hit))

    def doc_type(
        self, *doc_type: Union[type, str], **kwargs: Callable[[AttrDict[Any]], Any]
    ) -> Self:
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

    def using(self, client: AnyUsingType) -> Self:
        """
        Associate the search request with an elasticsearch client. A fresh copy
        will be returned with current instance remaining unchanged.

        :arg client: an instance of ``elasticsearch.Elasticsearch`` to use or
            an alias to look up in ``elasticsearch.dsl.connections``

        """
        s = self._clone()
        s._using = client
        return s

    def extra(self, **kwargs: Any) -> Self:
        """
        Add extra keys to the request body. Mostly here for backwards
        compatibility.
        """
        s = self._clone()
        if "from_" in kwargs:
            kwargs["from"] = kwargs.pop("from_")
        s._extra.update(kwargs)
        return s

    def _clone(self) -> Self:
        s = self.__class__(
            using=self._using, index=self._index, doc_type=self._doc_type
        )
        s._doc_type_map = self._doc_type_map.copy()
        s._extra = self._extra.copy()
        s._params = self._params.copy()
        return s

    if TYPE_CHECKING:

        def to_dict(self) -> Dict[str, Any]: ...


class SearchBase(Request[_R]):
    query = ProxyDescriptor[Self]("query")
    post_filter = ProxyDescriptor[Self]("post_filter")
    _response: Response[_R]

    def __init__(
        self,
        using: AnyUsingType = "default",
        index: Optional[Union[str, List[str]]] = None,
        **kwargs: Any,
    ):
        """
        Search request to elasticsearch.

        :arg using: `Elasticsearch` instance to use
        :arg index: limit the search to index

        All the parameters supplied (or omitted) at creation type can be later
        overridden by methods (`using`, `index` and `doc_type` respectively).
        """
        super().__init__(using=using, index=index, **kwargs)

        self.aggs = AggsProxy[_R](self)
        self._sort: List[Union[str, Dict[str, Dict[str, str]]]] = []
        self._knn: List[Dict[str, Any]] = []
        self._rank: Dict[str, Any] = {}
        self._collapse: Dict[str, Any] = {}
        self._source: Optional[Union[bool, List[str], Dict[str, List[str]]]] = None
        self._highlight: Dict[str, Any] = {}
        self._highlight_opts: Dict[str, Any] = {}
        self._suggest: Dict[str, Any] = {}
        self._script_fields: Dict[str, Any] = {}
        self._response_class = Response[_R]

        self._query_proxy = QueryProxy(self, "query")
        self._post_filter_proxy = QueryProxy(self, "post_filter")

    def filter(self, *args: Any, **kwargs: Any) -> Self:
        """
        Add a query in filter context.
        """
        return self.query(Bool(filter=[Q(*args, **kwargs)]))

    def exclude(self, *args: Any, **kwargs: Any) -> Self:
        """
        Add a negative query in filter context.
        """
        return self.query(Bool(filter=[~Q(*args, **kwargs)]))

    def __getitem__(self, n: Union[int, slice]) -> Self:
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
            slice_start = n.start
            slice_stop = n.stop
        else:  # This is an index lookup, equivalent to slicing by [n:n+1].
            # If negative index, abort.
            if n < 0:
                raise ValueError("Search does not support negative indexing.")
            slice_start = n
            slice_stop = n + 1

        old_from = s._extra.get("from")
        old_to = None
        if "size" in s._extra:
            old_to = (old_from or 0) + s._extra["size"]

        new_from = old_from
        if slice_start is not None:
            new_from = (old_from or 0) + slice_start
        new_to = old_to
        if slice_stop is not None:
            new_to = (old_from or 0) + slice_stop
            if old_to is not None and old_to < new_to:
                new_to = old_to

        if new_from is not None:
            s._extra["from"] = new_from
        if new_to is not None:
            s._extra["size"] = max(0, new_to - (new_from or 0))
        return s

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
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

    def _clone(self) -> Self:
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """
        s = super()._clone()

        s._response_class = self._response_class
        s._knn = [knn.copy() for knn in self._knn]
        s._rank = self._rank.copy()
        s._collapse = self._collapse.copy()
        s._sort = self._sort[:]
        s._source = copy.copy(self._source) if self._source is not None else None
        s._highlight = self._highlight.copy()
        s._highlight_opts = self._highlight_opts.copy()
        s._suggest = self._suggest.copy()
        s._script_fields = self._script_fields.copy()
        for x in ("query", "post_filter"):
            getattr(s, x)._proxied = getattr(self, x)._proxied

        # copy top-level bucket definitions
        if self.aggs._params.get("aggs"):
            s.aggs._params = {"aggs": self.aggs._params["aggs"].copy()}
        return s

    def response_class(self, cls: Type[Response[_R]]) -> Self:
        """
        Override the default wrapper used for the response.
        """
        s = self._clone()
        s._response_class = cls
        return s

    def update_from_dict(self, d: Dict[str, Any]) -> Self:
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
        if "knn" in d:
            self._knn = d.pop("knn")
            if isinstance(self._knn, dict):
                self._knn = [self._knn]
        if "rank" in d:
            self._rank = d.pop("rank")
        if "collapse" in d:
            self._collapse = d.pop("collapse")
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

    def script_fields(self, **kwargs: Any) -> Self:
        """
        Define script fields to be calculated on hits. See
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-script-fields.html
        for more details.

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

    def knn(
        self,
        field: Union[str, "InstrumentedField"],
        k: int,
        num_candidates: int,
        query_vector: Optional[List[float]] = None,
        query_vector_builder: Optional[Dict[str, Any]] = None,
        boost: Optional[float] = None,
        filter: Optional[Query] = None,
        similarity: Optional[float] = None,
        inner_hits: Optional[Dict[str, Any]] = None,
    ) -> Self:
        """
        Add a k-nearest neighbor (kNN) search.

        :arg field: the vector field to search against as a string or document class attribute
        :arg k: number of nearest neighbors to return as top hits
        :arg num_candidates: number of nearest neighbor candidates to consider per shard
        :arg query_vector: the vector to search for
        :arg query_vector_builder: A dictionary indicating how to build a query vector
        :arg boost: A floating-point boost factor for kNN scores
        :arg filter: query to filter the documents that can match
        :arg similarity: the minimum similarity required for a document to be considered a match, as a float value
        :arg inner_hits: retrieve hits from nested field

        Example::

            s = Search()
            s = s.knn(field='embedding', k=5, num_candidates=10, query_vector=vector,
                      filter=Q('term', category='blog')))
        """
        s = self._clone()
        s._knn.append(
            {
                "field": str(field),  # str() is for InstrumentedField instances
                "k": k,
                "num_candidates": num_candidates,
            }
        )
        if query_vector is None and query_vector_builder is None:
            raise ValueError("one of query_vector and query_vector_builder is required")
        if query_vector is not None and query_vector_builder is not None:
            raise ValueError(
                "only one of query_vector and query_vector_builder must be given"
            )
        if query_vector is not None:
            s._knn[-1]["query_vector"] = cast(Any, query_vector)
        if query_vector_builder is not None:
            s._knn[-1]["query_vector_builder"] = query_vector_builder
        if boost is not None:
            s._knn[-1]["boost"] = boost
        if filter is not None:
            if isinstance(filter, Query):
                s._knn[-1]["filter"] = filter.to_dict()
            else:
                s._knn[-1]["filter"] = filter
        if similarity is not None:
            s._knn[-1]["similarity"] = similarity
        if inner_hits is not None:
            s._knn[-1]["inner_hits"] = inner_hits
        return s

    def rank(self, rrf: Optional[Union[bool, Dict[str, Any]]] = None) -> Self:
        """
        Defines a method for combining and ranking results sets from a combination
        of searches. Requires a minimum of 2 results sets.

        :arg rrf: Set to ``True`` or an options dictionary to set the rank method to reciprocal rank fusion (RRF).

        Example::

            s = Search()
            s = s.query('match', content='search text')
            s = s.knn(field='embedding', k=5, num_candidates=10, query_vector=vector)
            s = s.rank(rrf=True)

        Note: This option is in technical preview and may change in the future. The syntax will likely change before GA.
        """
        s = self._clone()
        s._rank = {}
        if rrf is not None and rrf is not False:
            s._rank["rrf"] = {} if rrf is True else rrf
        return s

    def source(
        self,
        fields: Optional[
            Union[
                bool,
                str,
                "InstrumentedField",
                List[Union[str, "InstrumentedField"]],
                Dict[str, List[Union[str, "InstrumentedField"]]],
            ]
        ] = None,
        **kwargs: Any,
    ) -> Self:
        """
        Selectively control how the _source field is returned.

        :arg fields: field name, wildcard string, list of field names or wildcards,
                     or dictionary of includes and excludes
        :arg kwargs: ``includes`` or ``excludes`` arguments, when ``fields`` is ``None``.

        When no arguments are given, the entire document will be returned for
        each hit.  If ``fields`` is a string or list of strings, the field names or field
        wildcards given will be included. If ``fields`` is a dictionary with keys of
        'includes' and/or 'excludes' the fields will be either included or excluded
        appropriately.

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

        @overload
        def ensure_strings(fields: str) -> str: ...

        @overload
        def ensure_strings(fields: "InstrumentedField") -> str: ...

        @overload
        def ensure_strings(
            fields: List[Union[str, "InstrumentedField"]],
        ) -> List[str]: ...

        @overload
        def ensure_strings(
            fields: Dict[str, List[Union[str, "InstrumentedField"]]],
        ) -> Dict[str, List[str]]: ...

        def ensure_strings(
            fields: Union[
                str,
                "InstrumentedField",
                List[Union[str, "InstrumentedField"]],
                Dict[str, List[Union[str, "InstrumentedField"]]],
            ],
        ) -> Union[str, List[str], Dict[str, List[str]]]:
            if isinstance(fields, dict):
                return {k: ensure_strings(v) for k, v in fields.items()}
            elif not isinstance(fields, (str, InstrumentedField)):
                # we assume that if `fields` is not a any of [dict, str,
                # InstrumentedField] then it is an iterable of strings or
                # InstrumentedFields, so we convert them to a plain list of
                # strings
                return [str(f) for f in fields]
            else:
                return str(fields)

        if fields is not None:
            s._source = fields if isinstance(fields, bool) else ensure_strings(fields)  # type: ignore[assignment]
            return s

        if kwargs and not isinstance(s._source, dict):
            s._source = {}

        if isinstance(s._source, dict):
            for key, value in kwargs.items():
                if value is None:
                    try:
                        del s._source[key]
                    except KeyError:
                        pass
                else:
                    s._source[key] = ensure_strings(value)

        return s

    def sort(
        self, *keys: Union[str, "InstrumentedField", Dict[str, Dict[str, str]]]
    ) -> Self:
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
            if not isinstance(k, dict):
                sort_field = str(k)
                if sort_field.startswith("-"):
                    if sort_field[1:] == "_score":
                        raise IllegalOperation("Sorting by `-_score` is not allowed.")
                    s._sort.append({sort_field[1:]: {"order": "desc"}})
                else:
                    s._sort.append(sort_field)
            else:
                s._sort.append(k)
        return s

    def collapse(
        self,
        field: Optional[Union[str, "InstrumentedField"]] = None,
        inner_hits: Optional[Dict[str, Any]] = None,
        max_concurrent_group_searches: Optional[int] = None,
    ) -> Self:
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

        s._collapse["field"] = str(field)
        if inner_hits:
            s._collapse["inner_hits"] = inner_hits
        if max_concurrent_group_searches:
            s._collapse["max_concurrent_group_searches"] = max_concurrent_group_searches
        return s

    def highlight_options(self, **kwargs: Any) -> Self:
        """
        Update the global highlighting options used for this request. For
        example::

            s = Search()
            s = s.highlight_options(order='score')
        """
        s = self._clone()
        s._highlight_opts.update(kwargs)
        return s

    def highlight(
        self, *fields: Union[str, "InstrumentedField"], **kwargs: Any
    ) -> Self:
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
            s._highlight[str(f)] = kwargs
        return s

    def suggest(
        self,
        name: str,
        text: Optional[str] = None,
        regex: Optional[str] = None,
        **kwargs: Any,
    ) -> Self:
        """
        Add a suggestions request to the search.

        :arg name: name of the suggestion
        :arg text: text to suggest on

        All keyword arguments will be added to the suggestions body. For example::

            s = Search()
            s = s.suggest('suggestion-1', 'Elasticsearch', term={'field': 'body'})

        # regex query for Completion Suggester
            s = Search()
            s = s.suggest('suggestion-1', regex='py[thon|py]', completion={'field': 'body'})
        """
        if text is None and regex is None:
            raise ValueError('You have to pass "text" or "regex" argument.')
        if text and regex:
            raise ValueError('You can only pass either "text" or "regex" argument.')
        if regex and "completion" not in kwargs:
            raise ValueError(
                '"regex" argument must be passed with "completion" keyword argument.'
            )

        s = self._clone()
        if regex:
            s._suggest[name] = {"regex": regex}
        elif text:
            if "completion" in kwargs:
                s._suggest[name] = {"prefix": text}
            else:
                s._suggest[name] = {"text": text}
        s._suggest[name].update(kwargs)
        return s

    def search_after(self) -> Self:
        """
        Return a ``Search`` instance that retrieves the next page of results.

        This method provides an easy way to paginate a long list of results using
        the ``search_after`` option. For example::

            page_size = 20
            s = Search()[:page_size].sort("date")

            while True:
                # get a page of results
                r = await s.execute()

                # do something with this page of results

                # exit the loop if we reached the end
                if len(r.hits) < page_size:
                    break

                # get a search object with the next page of results
                s = s.search_after()

        Note that the ``search_after`` option requires the search to have an
        explicit ``sort`` order.
        """
        if not hasattr(self, "_response"):
            raise ValueError("A search must be executed before using search_after")
        return cast(Self, self._response.search_after())

    def to_dict(self, count: bool = False, **kwargs: Any) -> Dict[str, Any]:
        """
        Serialize the search into the dictionary that will be sent over as the
        request's body.

        :arg count: a flag to specify if we are interested in a body for count -
            no aggregations, no pagination bounds etc.

        All additional keyword arguments will be included into the dictionary.
        """
        d = {}

        if self.query:
            d["query"] = recursive_to_dict(self.query)

        if self._knn:
            if len(self._knn) == 1:
                d["knn"] = self._knn[0]
            else:
                d["knn"] = self._knn

        if self._rank:
            d["rank"] = self._rank

        # count request doesn't care for sorting and other things
        if not count:
            if self.post_filter:
                d["post_filter"] = recursive_to_dict(self.post_filter.to_dict())

            if self.aggs.aggs:
                d.update(recursive_to_dict(self.aggs.to_dict()))

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


class MultiSearchBase(Request[_R]):
    """
    Combine multiple :class:`~elasticsearch.dsl.Search` objects into a single
    request.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._searches: List[SearchBase[_R]] = []

    def __getitem__(self, key: Union[int, slice]) -> Any:
        return self._searches[key]

    def __iter__(self) -> Iterator[SearchBase[_R]]:
        return iter(self._searches)

    def _clone(self) -> Self:
        ms = super()._clone()
        ms._searches = self._searches[:]
        return ms

    def add(self, search: SearchBase[_R]) -> Self:
        """
        Adds a new :class:`~elasticsearch.dsl.Search` object to the request::

            ms = MultiSearch(index='my-index')
            ms = ms.add(Search(doc_type=Category).filter('term', category='python'))
            ms = ms.add(Search(doc_type=Blog))
        """
        ms = self._clone()
        ms._searches.append(search)
        return ms

    def to_dict(self) -> List[Dict[str, Any]]:  # type: ignore[override]
        out: List[Dict[str, Any]] = []
        for s in self._searches:
            meta: Dict[str, Any] = {}
            if s._index:
                meta["index"] = cast(Any, s._index)
            meta.update(s._params)

            out.append(meta)
            out.append(s.to_dict())

        return out
