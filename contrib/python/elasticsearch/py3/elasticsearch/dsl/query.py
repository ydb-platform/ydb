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
from copy import deepcopy
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)

from elastic_transport.client_utils import DEFAULT

# 'SF' looks unused but the test suite assumes it's available
# from this module so others are liable to do so as well.
from .function import SF  # noqa: F401
from .function import ScoreFunction
from .utils import DslBase

if TYPE_CHECKING:
    from elastic_transport.client_utils import DefaultType

    from . import types, wrappers
    from .document_base import InstrumentedField

_T = TypeVar("_T")
_M = TypeVar("_M", bound=Mapping[str, Any])


class QProxiedProtocol(Protocol[_T]):
    _proxied: _T


@overload
def Q(name_or_query: MutableMapping[str, _M]) -> "Query": ...


@overload
def Q(name_or_query: "Query") -> "Query": ...


@overload
def Q(name_or_query: QProxiedProtocol[_T]) -> _T: ...


@overload
def Q(name_or_query: str = "match_all", **params: Any) -> "Query": ...


def Q(
    name_or_query: Union[
        str,
        "Query",
        QProxiedProtocol[_T],
        MutableMapping[str, _M],
    ] = "match_all",
    **params: Any,
) -> Union["Query", _T]:
    # {"match": {"title": "python"}}
    if isinstance(name_or_query, collections.abc.MutableMapping):
        if params:
            raise ValueError("Q() cannot accept parameters when passing in a dict.")
        if len(name_or_query) != 1:
            raise ValueError(
                'Q() can only accept dict with a single query ({"match": {...}}). '
                "Instead it got (%r)" % name_or_query
            )
        name, q_params = deepcopy(name_or_query).popitem()
        return Query.get_dsl_class(name)(_expand__to_dot=False, **q_params)

    # MatchAll()
    if isinstance(name_or_query, Query):
        if params:
            raise ValueError(
                "Q() cannot accept parameters when passing in a Query object."
            )
        return name_or_query

    # s.query = Q('filtered', query=s.query)
    if hasattr(name_or_query, "_proxied"):
        return cast(QProxiedProtocol[_T], name_or_query)._proxied

    # "match", title="python"
    return Query.get_dsl_class(name_or_query)(**params)


class Query(DslBase):
    _type_name = "query"
    _type_shortcut = staticmethod(Q)
    name: ClassVar[Optional[str]] = None

    # Add type annotations for methods not defined in every subclass
    __ror__: ClassVar[Callable[["Query", "Query"], "Query"]]
    __radd__: ClassVar[Callable[["Query", "Query"], "Query"]]
    __rand__: ClassVar[Callable[["Query", "Query"], "Query"]]

    def __add__(self, other: "Query") -> "Query":
        # make sure we give queries that know how to combine themselves
        # preference
        if hasattr(other, "__radd__"):
            return other.__radd__(self)
        return Bool(must=[self, other])

    def __invert__(self) -> "Query":
        return Bool(must_not=[self])

    def __or__(self, other: "Query") -> "Query":
        # make sure we give queries that know how to combine themselves
        # preference
        if hasattr(other, "__ror__"):
            return other.__ror__(self)
        return Bool(should=[self, other])

    def __and__(self, other: "Query") -> "Query":
        # make sure we give queries that know how to combine themselves
        # preference
        if hasattr(other, "__rand__"):
            return other.__rand__(self)
        return Bool(must=[self, other])


class Bool(Query):
    """
    matches documents matching boolean combinations of other queries.

    :arg filter: The clause (query) must appear in matching documents.
        However, unlike `must`, the score of the query will be ignored.
    :arg minimum_should_match: Specifies the number or percentage of
        `should` clauses returned documents must match.
    :arg must: The clause (query) must appear in matching documents and
        will contribute to the score.
    :arg must_not: The clause (query) must not appear in the matching
        documents. Because scoring is ignored, a score of `0` is returned
        for all documents.
    :arg should: The clause (query) should appear in the matching
        document.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "bool"
    _param_defs = {
        "filter": {"type": "query", "multi": True},
        "must": {"type": "query", "multi": True},
        "must_not": {"type": "query", "multi": True},
        "should": {"type": "query", "multi": True},
    }

    def __init__(
        self,
        *,
        filter: Union[Query, Sequence[Query], "DefaultType"] = DEFAULT,
        minimum_should_match: Union[int, str, "DefaultType"] = DEFAULT,
        must: Union[Query, Sequence[Query], "DefaultType"] = DEFAULT,
        must_not: Union[Query, Sequence[Query], "DefaultType"] = DEFAULT,
        should: Union[Query, Sequence[Query], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            filter=filter,
            minimum_should_match=minimum_should_match,
            must=must,
            must_not=must_not,
            should=should,
            boost=boost,
            _name=_name,
            **kwargs,
        )

    def __add__(self, other: Query) -> "Bool":
        q = self._clone()
        if isinstance(other, Bool):
            q.must += other.must
            q.should += other.should
            q.must_not += other.must_not
            q.filter += other.filter
        else:
            q.must.append(other)
        return q

    __radd__ = __add__

    def __or__(self, other: Query) -> Query:
        for q in (self, other):
            if isinstance(q, Bool) and not any(
                (q.must, q.must_not, q.filter, getattr(q, "minimum_should_match", None))
            ):
                other = self if q is other else other
                q = q._clone()
                if isinstance(other, Bool) and not any(
                    (
                        other.must,
                        other.must_not,
                        other.filter,
                        getattr(other, "minimum_should_match", None),
                    )
                ):
                    q.should.extend(other.should)
                else:
                    q.should.append(other)
                return q

        return Bool(should=[self, other])

    __ror__ = __or__

    @property
    def _min_should_match(self) -> int:
        return getattr(
            self,
            "minimum_should_match",
            0 if not self.should or (self.must or self.filter) else 1,
        )

    def __invert__(self) -> Query:
        # Because an empty Bool query is treated like
        # MatchAll the inverse should be MatchNone
        if not any(chain(self.must, self.filter, self.should, self.must_not)):
            return MatchNone()

        negations: List[Query] = []
        for q in chain(self.must, self.filter):
            negations.append(~q)

        for q in self.must_not:
            negations.append(q)

        if self.should and self._min_should_match:
            negations.append(Bool(must_not=self.should[:]))

        if len(negations) == 1:
            return negations[0]
        return Bool(should=negations)

    def __and__(self, other: Query) -> Query:
        q = self._clone()
        if isinstance(other, Bool):
            q.must += other.must
            q.must_not += other.must_not
            q.filter += other.filter
            q.should = []

            # reset minimum_should_match as it will get calculated below
            if "minimum_should_match" in q._params:
                del q._params["minimum_should_match"]

            for qx in (self, other):
                min_should_match = qx._min_should_match
                # TODO: percentages or negative numbers will fail here
                # for now we report an error
                if not isinstance(min_should_match, int) or min_should_match < 0:
                    raise ValueError(
                        "Can only combine queries with positive integer values for minimum_should_match"
                    )
                # all subqueries are required
                if len(qx.should) <= min_should_match:
                    q.must.extend(qx.should)
                # not all of them are required, use it and remember min_should_match
                elif not q.should:
                    q.minimum_should_match = min_should_match
                    q.should = qx.should
                # all queries are optional, just extend should
                elif q._min_should_match == 0 and min_should_match == 0:
                    q.should.extend(qx.should)
                # not all are required, add a should list to the must with proper min_should_match
                else:
                    q.must.append(
                        Bool(should=qx.should, minimum_should_match=min_should_match)
                    )
        else:
            if not (q.must or q.filter) and q.should:
                q._params.setdefault("minimum_should_match", 1)
            q.must.append(other)
        return q

    __rand__ = __and__


class Boosting(Query):
    """
    Returns documents matching a `positive` query while reducing the
    relevance score of documents that also match a `negative` query.

    :arg negative_boost: (required) Floating point number between 0 and
        1.0 used to decrease the relevance scores of documents matching
        the `negative` query.
    :arg negative: (required) Query used to decrease the relevance score
        of matching documents.
    :arg positive: (required) Any returned documents must match this
        query.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "boosting"
    _param_defs = {
        "negative": {"type": "query"},
        "positive": {"type": "query"},
    }

    def __init__(
        self,
        *,
        negative_boost: Union[float, "DefaultType"] = DEFAULT,
        negative: Union[Query, "DefaultType"] = DEFAULT,
        positive: Union[Query, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            negative_boost=negative_boost,
            negative=negative,
            positive=positive,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Common(Query):
    """
    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "common"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.CommonTermsQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class CombinedFields(Query):
    """
    The `combined_fields` query supports searching multiple text fields as
    if their contents had been indexed into one combined field.

    :arg fields: (required) List of fields to search. Field wildcard
        patterns are allowed. Only `text` fields are supported, and they
        must all have the same search `analyzer`.
    :arg query: (required) Text to search for in the provided `fields`.
        The `combined_fields` query analyzes the provided text before
        performing a search.
    :arg auto_generate_synonyms_phrase_query: If true, match phrase
        queries are automatically created for multi-term synonyms.
        Defaults to `True` if omitted.
    :arg operator: Boolean logic used to interpret text in the query
        value. Defaults to `or` if omitted.
    :arg minimum_should_match: Minimum number of clauses that must match
        for a document to be returned.
    :arg zero_terms_query: Indicates whether no documents are returned if
        the analyzer removes all tokens, such as when using a `stop`
        filter. Defaults to `none` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "combined_fields"

    def __init__(
        self,
        *,
        fields: Union[
            Sequence[Union[str, "InstrumentedField"]], "DefaultType"
        ] = DEFAULT,
        query: Union[str, "DefaultType"] = DEFAULT,
        auto_generate_synonyms_phrase_query: Union[bool, "DefaultType"] = DEFAULT,
        operator: Union[Literal["or", "and"], "DefaultType"] = DEFAULT,
        minimum_should_match: Union[int, str, "DefaultType"] = DEFAULT,
        zero_terms_query: Union[Literal["none", "all"], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            fields=fields,
            query=query,
            auto_generate_synonyms_phrase_query=auto_generate_synonyms_phrase_query,
            operator=operator,
            minimum_should_match=minimum_should_match,
            zero_terms_query=zero_terms_query,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class ConstantScore(Query):
    """
    Wraps a filter query and returns every matching document with a
    relevance score equal to the `boost` parameter value.

    :arg filter: (required) Filter query you wish to run. Any returned
        documents must match this query. Filter queries do not calculate
        relevance scores. To speed up performance, Elasticsearch
        automatically caches frequently used filter queries.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "constant_score"
    _param_defs = {
        "filter": {"type": "query"},
    }

    def __init__(
        self,
        *,
        filter: Union[Query, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(filter=filter, boost=boost, _name=_name, **kwargs)


class DisMax(Query):
    """
    Returns documents matching one or more wrapped queries, called query
    clauses or clauses. If a returned document matches multiple query
    clauses, the `dis_max` query assigns the document the highest
    relevance score from any matching clause, plus a tie breaking
    increment for any additional matching subqueries.

    :arg queries: (required) One or more query clauses. Returned documents
        must match one or more of these queries. If a document matches
        multiple queries, Elasticsearch uses the highest relevance score.
    :arg tie_breaker: Floating point number between 0 and 1.0 used to
        increase the relevance scores of documents matching multiple query
        clauses.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "dis_max"
    _param_defs = {
        "queries": {"type": "query", "multi": True},
    }

    def __init__(
        self,
        *,
        queries: Union[Sequence[Query], "DefaultType"] = DEFAULT,
        tie_breaker: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            queries=queries, tie_breaker=tie_breaker, boost=boost, _name=_name, **kwargs
        )


class DistanceFeature(Query):
    """
    Boosts the relevance score of documents closer to a provided origin
    date or point. For example, you can use this query to give more weight
    to documents closer to a certain date or location.

    :arg origin: (required) Date or point of origin used to calculate
        distances. If the `field` value is a `date` or `date_nanos` field,
        the `origin` value must be a date. Date Math, such as `now-1h`, is
        supported. If the field value is a `geo_point` field, the `origin`
        value must be a geopoint.
    :arg pivot: (required) Distance from the `origin` at which relevance
        scores receive half of the `boost` value. If the `field` value is
        a `date` or `date_nanos` field, the `pivot` value must be a time
        unit, such as `1h` or `10d`. If the `field` value is a `geo_point`
        field, the `pivot` value must be a distance unit, such as `1km` or
        `12m`.
    :arg field: (required) Name of the field used to calculate distances.
        This field must meet the following criteria: be a `date`,
        `date_nanos` or `geo_point` field; have an `index` mapping
        parameter value of `true`, which is the default; have an
        `doc_values` mapping parameter value of `true`, which is the
        default.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "distance_feature"

    def __init__(
        self,
        *,
        origin: Any = DEFAULT,
        pivot: Any = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            origin=origin, pivot=pivot, field=field, boost=boost, _name=_name, **kwargs
        )


class Exists(Query):
    """
    Returns documents that contain an indexed value for a field.

    :arg field: (required) Name of the field you wish to search.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "exists"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(field=field, boost=boost, _name=_name, **kwargs)


class FunctionScore(Query):
    """
    The `function_score` enables you to modify the score of documents that
    are retrieved by a query.

    :arg boost_mode: Defines how he newly computed score is combined with
        the score of the query Defaults to `multiply` if omitted.
    :arg functions: One or more functions that compute a new score for
        each document returned by the query.
    :arg max_boost: Restricts the new score to not exceed the provided
        limit.
    :arg min_score: Excludes documents that do not meet the provided score
        threshold.
    :arg query: A query that determines the documents for which a new
        score is computed.
    :arg score_mode: Specifies how the computed scores are combined
        Defaults to `multiply` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "function_score"
    _param_defs = {
        "functions": {"type": "score_function", "multi": True},
        "query": {"type": "query"},
        "filter": {"type": "query"},
    }

    def __init__(
        self,
        *,
        boost_mode: Union[
            Literal["multiply", "replace", "sum", "avg", "max", "min"], "DefaultType"
        ] = DEFAULT,
        functions: Union[Sequence[ScoreFunction], "DefaultType"] = DEFAULT,
        max_boost: Union[float, "DefaultType"] = DEFAULT,
        min_score: Union[float, "DefaultType"] = DEFAULT,
        query: Union[Query, "DefaultType"] = DEFAULT,
        score_mode: Union[
            Literal["multiply", "sum", "avg", "first", "max", "min"], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if functions is DEFAULT:
            functions = []
            for name in ScoreFunction._classes:
                if name in kwargs:
                    functions.append({name: kwargs.pop(name)})  # type: ignore[arg-type]
        super().__init__(
            boost_mode=boost_mode,
            functions=functions,
            max_boost=max_boost,
            min_score=min_score,
            query=query,
            score_mode=score_mode,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Fuzzy(Query):
    """
    Returns documents that contain terms similar to the search term, as
    measured by a Levenshtein edit distance.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "fuzzy"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.FuzzyQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class GeoBoundingBox(Query):
    """
    Matches geo_point and geo_shape values that intersect a bounding box.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg type:
    :arg validation_method: Set to `IGNORE_MALFORMED` to accept geo points
        with invalid latitude or longitude. Set to `COERCE` to also try to
        infer correct latitude or longitude. Defaults to `'strict'` if
        omitted.
    :arg ignore_unmapped: Set to `true` to ignore an unmapped field and
        not match any documents for this query. Set to `false` to throw an
        exception if the field is not mapped.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "geo_bounding_box"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.CoordsGeoBounds",
            "types.TopLeftBottomRightGeoBounds",
            "types.TopRightBottomLeftGeoBounds",
            "types.WktGeoBounds",
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        *,
        type: Union[Literal["memory", "indexed"], "DefaultType"] = DEFAULT,
        validation_method: Union[
            Literal["coerce", "ignore_malformed", "strict"], "DefaultType"
        ] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(
            type=type,
            validation_method=validation_method,
            ignore_unmapped=ignore_unmapped,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class GeoDistance(Query):
    """
    Matches `geo_point` and `geo_shape` values within a given distance of
    a geopoint.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg distance: (required) The radius of the circle centred on the
        specified location. Points which fall into this circle are
        considered to be matches.
    :arg distance_type: How to compute the distance. Set to `plane` for a
        faster calculation that's inaccurate on long distances and close
        to the poles. Defaults to `'arc'` if omitted.
    :arg validation_method: Set to `IGNORE_MALFORMED` to accept geo points
        with invalid latitude or longitude. Set to `COERCE` to also try to
        infer correct latitude or longitude. Defaults to `'strict'` if
        omitted.
    :arg ignore_unmapped: Set to `true` to ignore an unmapped field and
        not match any documents for this query. Set to `false` to throw an
        exception if the field is not mapped.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "geo_distance"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.LatLonGeoLocation",
            "types.GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        *,
        distance: Union[str, "DefaultType"] = DEFAULT,
        distance_type: Union[Literal["arc", "plane"], "DefaultType"] = DEFAULT,
        validation_method: Union[
            Literal["coerce", "ignore_malformed", "strict"], "DefaultType"
        ] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(
            distance=distance,
            distance_type=distance_type,
            validation_method=validation_method,
            ignore_unmapped=ignore_unmapped,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class GeoGrid(Query):
    """
    Matches `geo_point` and `geo_shape` values that intersect a grid cell
    from a GeoGrid aggregation.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "geo_grid"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.GeoGridQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class GeoPolygon(Query):
    """
    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg validation_method:  Defaults to `'strict'` if omitted.
    :arg ignore_unmapped:
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "geo_polygon"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.GeoPolygonPoints", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        *,
        validation_method: Union[
            Literal["coerce", "ignore_malformed", "strict"], "DefaultType"
        ] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(
            validation_method=validation_method,
            ignore_unmapped=ignore_unmapped,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class GeoShape(Query):
    """
    Filter documents indexed using either the `geo_shape` or the
    `geo_point` type.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg ignore_unmapped: Set to `true` to ignore an unmapped field and
        not match any documents for this query. Set to `false` to throw an
        exception if the field is not mapped.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "geo_shape"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.GeoShapeFieldQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        *,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(
            ignore_unmapped=ignore_unmapped, boost=boost, _name=_name, **kwargs
        )


class HasChild(Query):
    """
    Returns parent documents whose joined child documents match a provided
    query.

    :arg query: (required) Query you wish to run on child documents of the
        `type` field. If a child document matches the search, the query
        returns the parent document.
    :arg type: (required) Name of the child relationship mapped for the
        `join` field.
    :arg ignore_unmapped: Indicates whether to ignore an unmapped `type`
        and not return any documents instead of an error.
    :arg inner_hits: If defined, each search hit will contain inner hits.
    :arg max_children: Maximum number of child documents that match the
        query allowed for a returned parent document. If the parent
        document exceeds this limit, it is excluded from the search
        results.
    :arg min_children: Minimum number of child documents that match the
        query required to match the query for a returned parent document.
        If the parent document does not meet this limit, it is excluded
        from the search results.
    :arg score_mode: Indicates how scores for matching child documents
        affect the root parent document’s relevance score. Defaults to
        `'none'` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "has_child"
    _param_defs = {
        "query": {"type": "query"},
    }

    def __init__(
        self,
        *,
        query: Union[Query, "DefaultType"] = DEFAULT,
        type: Union[str, "DefaultType"] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        inner_hits: Union["types.InnerHits", Dict[str, Any], "DefaultType"] = DEFAULT,
        max_children: Union[int, "DefaultType"] = DEFAULT,
        min_children: Union[int, "DefaultType"] = DEFAULT,
        score_mode: Union[
            Literal["none", "avg", "sum", "max", "min"], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            query=query,
            type=type,
            ignore_unmapped=ignore_unmapped,
            inner_hits=inner_hits,
            max_children=max_children,
            min_children=min_children,
            score_mode=score_mode,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class HasParent(Query):
    """
    Returns child documents whose joined parent document matches a
    provided query.

    :arg parent_type: (required) Name of the parent relationship mapped
        for the `join` field.
    :arg query: (required) Query you wish to run on parent documents of
        the `parent_type` field. If a parent document matches the search,
        the query returns its child documents.
    :arg ignore_unmapped: Indicates whether to ignore an unmapped
        `parent_type` and not return any documents instead of an error.
        You can use this parameter to query multiple indices that may not
        contain the `parent_type`.
    :arg inner_hits: If defined, each search hit will contain inner hits.
    :arg score: Indicates whether the relevance score of a matching parent
        document is aggregated into its child documents.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "has_parent"
    _param_defs = {
        "query": {"type": "query"},
    }

    def __init__(
        self,
        *,
        parent_type: Union[str, "DefaultType"] = DEFAULT,
        query: Union[Query, "DefaultType"] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        inner_hits: Union["types.InnerHits", Dict[str, Any], "DefaultType"] = DEFAULT,
        score: Union[bool, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            parent_type=parent_type,
            query=query,
            ignore_unmapped=ignore_unmapped,
            inner_hits=inner_hits,
            score=score,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Ids(Query):
    """
    Returns documents based on their IDs. This query uses document IDs
    stored in the `_id` field.

    :arg values: An array of document IDs.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "ids"

    def __init__(
        self,
        *,
        values: Union[str, Sequence[str], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(values=values, boost=boost, _name=_name, **kwargs)


class Intervals(Query):
    """
    Returns documents based on the order and proximity of matching terms.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "intervals"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.IntervalsQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class Knn(Query):
    """
    Finds the k nearest vectors to a query vector, as measured by a
    similarity metric. knn query finds nearest vectors through approximate
    search on indexed dense_vectors.

    :arg field: (required) The name of the vector field to search against
    :arg query_vector: The query vector
    :arg query_vector_builder: The query vector builder. You must provide
        a query_vector_builder or query_vector, but not both.
    :arg num_candidates: The number of nearest neighbor candidates to
        consider per shard
    :arg k: The final number of nearest neighbors to return as top hits
    :arg filter: Filters for the kNN search query
    :arg similarity: The minimum similarity for a vector to be considered
        a match
    :arg rescore_vector: Apply oversampling and rescoring to quantized
        vectors
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "knn"
    _param_defs = {
        "filter": {"type": "query", "multi": True},
    }

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        query_vector: Union[Sequence[float], "DefaultType"] = DEFAULT,
        query_vector_builder: Union[
            "types.QueryVectorBuilder", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        num_candidates: Union[int, "DefaultType"] = DEFAULT,
        k: Union[int, "DefaultType"] = DEFAULT,
        filter: Union[Query, Sequence[Query], "DefaultType"] = DEFAULT,
        similarity: Union[float, "DefaultType"] = DEFAULT,
        rescore_vector: Union[
            "types.RescoreVector", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            query_vector=query_vector,
            query_vector_builder=query_vector_builder,
            num_candidates=num_candidates,
            k=k,
            filter=filter,
            similarity=similarity,
            rescore_vector=rescore_vector,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Match(Query):
    """
    Returns documents that match a provided text, number, date or boolean
    value. The provided text is analyzed before matching.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "match"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.MatchQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class MatchAll(Query):
    """
    Matches all documents, giving them all a `_score` of 1.0.

    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "match_all"

    def __init__(
        self,
        *,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(boost=boost, _name=_name, **kwargs)

    def __add__(self, other: "Query") -> "Query":
        return other._clone()

    __and__ = __rand__ = __radd__ = __add__

    def __or__(self, other: "Query") -> "MatchAll":
        return self

    __ror__ = __or__

    def __invert__(self) -> "MatchNone":
        return MatchNone()


EMPTY_QUERY = MatchAll()


class MatchBoolPrefix(Query):
    """
    Analyzes its input and constructs a `bool` query from the terms. Each
    term except the last is used in a `term` query. The last term is used
    in a prefix query.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "match_bool_prefix"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.MatchBoolPrefixQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class MatchNone(Query):
    """
    Matches no documents.

    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "match_none"

    def __init__(
        self,
        *,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(boost=boost, _name=_name, **kwargs)

    def __add__(self, other: "Query") -> "MatchNone":
        return self

    __and__ = __rand__ = __radd__ = __add__

    def __or__(self, other: "Query") -> "Query":
        return other._clone()

    __ror__ = __or__

    def __invert__(self) -> MatchAll:
        return MatchAll()


class MatchPhrase(Query):
    """
    Analyzes the text and creates a phrase query out of the analyzed text.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "match_phrase"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.MatchPhraseQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class MatchPhrasePrefix(Query):
    """
    Returns documents that contain the words of a provided text, in the
    same order as provided. The last term of the provided text is treated
    as a prefix, matching any words that begin with that term.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "match_phrase_prefix"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.MatchPhrasePrefixQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class MoreLikeThis(Query):
    """
    Returns documents that are "like" a given set of documents.

    :arg like: (required) Specifies free form text and/or a single or
        multiple documents for which you want to find similar documents.
    :arg analyzer: The analyzer that is used to analyze the free form
        text. Defaults to the analyzer associated with the first field in
        fields.
    :arg boost_terms: Each term in the formed query could be further
        boosted by their tf-idf score. This sets the boost factor to use
        when using this feature. Defaults to deactivated (0).
    :arg fail_on_unsupported_field: Controls whether the query should fail
        (throw an exception) if any of the specified fields are not of the
        supported types (`text` or `keyword`). Defaults to `True` if
        omitted.
    :arg fields: A list of fields to fetch and analyze the text from.
        Defaults to the `index.query.default_field` index setting, which
        has a default value of `*`.
    :arg include: Specifies whether the input documents should also be
        included in the search results returned.
    :arg max_doc_freq: The maximum document frequency above which the
        terms are ignored from the input document.
    :arg max_query_terms: The maximum number of query terms that can be
        selected. Defaults to `25` if omitted.
    :arg max_word_length: The maximum word length above which the terms
        are ignored. Defaults to unbounded (`0`).
    :arg min_doc_freq: The minimum document frequency below which the
        terms are ignored from the input document. Defaults to `5` if
        omitted.
    :arg minimum_should_match: After the disjunctive query has been
        formed, this parameter controls the number of terms that must
        match.
    :arg min_term_freq: The minimum term frequency below which the terms
        are ignored from the input document. Defaults to `2` if omitted.
    :arg min_word_length: The minimum word length below which the terms
        are ignored.
    :arg routing:
    :arg stop_words: An array of stop words. Any word in this set is
        ignored.
    :arg unlike: Used in combination with `like` to exclude documents that
        match a set of terms.
    :arg version:
    :arg version_type:  Defaults to `'internal'` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "more_like_this"

    def __init__(
        self,
        *,
        like: Union[
            Union[str, "types.LikeDocument"],
            Sequence[Union[str, "types.LikeDocument"]],
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        analyzer: Union[str, "DefaultType"] = DEFAULT,
        boost_terms: Union[float, "DefaultType"] = DEFAULT,
        fail_on_unsupported_field: Union[bool, "DefaultType"] = DEFAULT,
        fields: Union[
            Sequence[Union[str, "InstrumentedField"]], "DefaultType"
        ] = DEFAULT,
        include: Union[bool, "DefaultType"] = DEFAULT,
        max_doc_freq: Union[int, "DefaultType"] = DEFAULT,
        max_query_terms: Union[int, "DefaultType"] = DEFAULT,
        max_word_length: Union[int, "DefaultType"] = DEFAULT,
        min_doc_freq: Union[int, "DefaultType"] = DEFAULT,
        minimum_should_match: Union[int, str, "DefaultType"] = DEFAULT,
        min_term_freq: Union[int, "DefaultType"] = DEFAULT,
        min_word_length: Union[int, "DefaultType"] = DEFAULT,
        routing: Union[str, "DefaultType"] = DEFAULT,
        stop_words: Union[
            Literal[
                "_arabic_",
                "_armenian_",
                "_basque_",
                "_bengali_",
                "_brazilian_",
                "_bulgarian_",
                "_catalan_",
                "_cjk_",
                "_czech_",
                "_danish_",
                "_dutch_",
                "_english_",
                "_estonian_",
                "_finnish_",
                "_french_",
                "_galician_",
                "_german_",
                "_greek_",
                "_hindi_",
                "_hungarian_",
                "_indonesian_",
                "_irish_",
                "_italian_",
                "_latvian_",
                "_lithuanian_",
                "_norwegian_",
                "_persian_",
                "_portuguese_",
                "_romanian_",
                "_russian_",
                "_serbian_",
                "_sorani_",
                "_spanish_",
                "_swedish_",
                "_thai_",
                "_turkish_",
                "_none_",
            ],
            Sequence[str],
            "DefaultType",
        ] = DEFAULT,
        unlike: Union[
            Union[str, "types.LikeDocument"],
            Sequence[Union[str, "types.LikeDocument"]],
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        version: Union[int, "DefaultType"] = DEFAULT,
        version_type: Union[
            Literal["internal", "external", "external_gte", "force"], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            like=like,
            analyzer=analyzer,
            boost_terms=boost_terms,
            fail_on_unsupported_field=fail_on_unsupported_field,
            fields=fields,
            include=include,
            max_doc_freq=max_doc_freq,
            max_query_terms=max_query_terms,
            max_word_length=max_word_length,
            min_doc_freq=min_doc_freq,
            minimum_should_match=minimum_should_match,
            min_term_freq=min_term_freq,
            min_word_length=min_word_length,
            routing=routing,
            stop_words=stop_words,
            unlike=unlike,
            version=version,
            version_type=version_type,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class MultiMatch(Query):
    """
    Enables you to search for a provided text, number, date or boolean
    value across multiple fields. The provided text is analyzed before
    matching.

    :arg query: (required) Text, number, boolean value or date you wish to
        find in the provided field.
    :arg analyzer: Analyzer used to convert the text in the query value
        into tokens.
    :arg auto_generate_synonyms_phrase_query: If `true`, match phrase
        queries are automatically created for multi-term synonyms.
        Defaults to `True` if omitted.
    :arg cutoff_frequency:
    :arg fields: The fields to be queried. Defaults to the
        `index.query.default_field` index settings, which in turn defaults
        to `*`.
    :arg fuzziness: Maximum edit distance allowed for matching.
    :arg fuzzy_rewrite: Method used to rewrite the query.
    :arg fuzzy_transpositions: If `true`, edits for fuzzy matching include
        transpositions of two adjacent characters (for example, `ab` to
        `ba`). Can be applied to the term subqueries constructed for all
        terms but the final term. Defaults to `True` if omitted.
    :arg lenient: If `true`, format-based errors, such as providing a text
        query value for a numeric field, are ignored.
    :arg max_expansions: Maximum number of terms to which the query will
        expand. Defaults to `50` if omitted.
    :arg minimum_should_match: Minimum number of clauses that must match
        for a document to be returned.
    :arg operator: Boolean logic used to interpret text in the query
        value. Defaults to `'or'` if omitted.
    :arg prefix_length: Number of beginning characters left unchanged for
        fuzzy matching.
    :arg slop: Maximum number of positions allowed between matching
        tokens.
    :arg tie_breaker: Determines how scores for each per-term blended
        query and scores across groups are combined.
    :arg type: How `the` multi_match query is executed internally.
        Defaults to `'best_fields'` if omitted.
    :arg zero_terms_query: Indicates whether no documents are returned if
        the `analyzer` removes all tokens, such as when using a `stop`
        filter. Defaults to `'none'` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "multi_match"

    def __init__(
        self,
        *,
        query: Union[str, "DefaultType"] = DEFAULT,
        analyzer: Union[str, "DefaultType"] = DEFAULT,
        auto_generate_synonyms_phrase_query: Union[bool, "DefaultType"] = DEFAULT,
        cutoff_frequency: Union[float, "DefaultType"] = DEFAULT,
        fields: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        fuzziness: Union[str, int, "DefaultType"] = DEFAULT,
        fuzzy_rewrite: Union[str, "DefaultType"] = DEFAULT,
        fuzzy_transpositions: Union[bool, "DefaultType"] = DEFAULT,
        lenient: Union[bool, "DefaultType"] = DEFAULT,
        max_expansions: Union[int, "DefaultType"] = DEFAULT,
        minimum_should_match: Union[int, str, "DefaultType"] = DEFAULT,
        operator: Union[Literal["and", "or"], "DefaultType"] = DEFAULT,
        prefix_length: Union[int, "DefaultType"] = DEFAULT,
        slop: Union[int, "DefaultType"] = DEFAULT,
        tie_breaker: Union[float, "DefaultType"] = DEFAULT,
        type: Union[
            Literal[
                "best_fields",
                "most_fields",
                "cross_fields",
                "phrase",
                "phrase_prefix",
                "bool_prefix",
            ],
            "DefaultType",
        ] = DEFAULT,
        zero_terms_query: Union[Literal["all", "none"], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            query=query,
            analyzer=analyzer,
            auto_generate_synonyms_phrase_query=auto_generate_synonyms_phrase_query,
            cutoff_frequency=cutoff_frequency,
            fields=fields,
            fuzziness=fuzziness,
            fuzzy_rewrite=fuzzy_rewrite,
            fuzzy_transpositions=fuzzy_transpositions,
            lenient=lenient,
            max_expansions=max_expansions,
            minimum_should_match=minimum_should_match,
            operator=operator,
            prefix_length=prefix_length,
            slop=slop,
            tie_breaker=tie_breaker,
            type=type,
            zero_terms_query=zero_terms_query,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Nested(Query):
    """
    Wraps another query to search nested fields. If an object matches the
    search, the nested query returns the root parent document.

    :arg path: (required) Path to the nested object you wish to search.
    :arg query: (required) Query you wish to run on nested objects in the
        path.
    :arg ignore_unmapped: Indicates whether to ignore an unmapped path and
        not return any documents instead of an error.
    :arg inner_hits: If defined, each search hit will contain inner hits.
    :arg score_mode: How scores for matching child objects affect the root
        parent document’s relevance score. Defaults to `'avg'` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "nested"
    _param_defs = {
        "query": {"type": "query"},
    }

    def __init__(
        self,
        *,
        path: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        query: Union[Query, "DefaultType"] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        inner_hits: Union["types.InnerHits", Dict[str, Any], "DefaultType"] = DEFAULT,
        score_mode: Union[
            Literal["none", "avg", "sum", "max", "min"], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            path=path,
            query=query,
            ignore_unmapped=ignore_unmapped,
            inner_hits=inner_hits,
            score_mode=score_mode,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class ParentId(Query):
    """
    Returns child documents joined to a specific parent document.

    :arg id: ID of the parent document.
    :arg ignore_unmapped: Indicates whether to ignore an unmapped `type`
        and not return any documents instead of an error.
    :arg type: Name of the child relationship mapped for the `join` field.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "parent_id"

    def __init__(
        self,
        *,
        id: Union[str, "DefaultType"] = DEFAULT,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        type: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            id=id,
            ignore_unmapped=ignore_unmapped,
            type=type,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Percolate(Query):
    """
    Matches queries stored in an index.

    :arg field: (required) Field that holds the indexed queries. The field
        must use the `percolator` mapping type.
    :arg document: The source of the document being percolated.
    :arg documents: An array of sources of the documents being percolated.
    :arg id: The ID of a stored document to percolate.
    :arg index: The index of a stored document to percolate.
    :arg name: The suffix used for the `_percolator_document_slot` field
        when multiple `percolate` queries are specified.
    :arg preference: Preference used to fetch document to percolate.
    :arg routing: Routing used to fetch document to percolate.
    :arg version: The expected version of a stored document to percolate.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "percolate"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        document: Any = DEFAULT,
        documents: Union[Sequence[Any], "DefaultType"] = DEFAULT,
        id: Union[str, "DefaultType"] = DEFAULT,
        index: Union[str, "DefaultType"] = DEFAULT,
        name: Union[str, "DefaultType"] = DEFAULT,
        preference: Union[str, "DefaultType"] = DEFAULT,
        routing: Union[str, "DefaultType"] = DEFAULT,
        version: Union[int, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            document=document,
            documents=documents,
            id=id,
            index=index,
            name=name,
            preference=preference,
            routing=routing,
            version=version,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Pinned(Query):
    """
    Promotes selected documents to rank higher than those matching a given
    query.

    :arg organic: (required) Any choice of query used to rank documents
        which will be ranked below the "pinned" documents.
    :arg ids: Document IDs listed in the order they are to appear in
        results. Required if `docs` is not specified.
    :arg docs: Documents listed in the order they are to appear in
        results. Required if `ids` is not specified.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "pinned"
    _param_defs = {
        "organic": {"type": "query"},
    }

    def __init__(
        self,
        *,
        organic: Union[Query, "DefaultType"] = DEFAULT,
        ids: Union[Sequence[str], "DefaultType"] = DEFAULT,
        docs: Union[
            Sequence["types.PinnedDoc"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            organic=organic, ids=ids, docs=docs, boost=boost, _name=_name, **kwargs
        )


class Prefix(Query):
    """
    Returns documents that contain a specific prefix in a provided field.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "prefix"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.PrefixQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class QueryString(Query):
    """
    Returns documents based on a provided query string, using a parser
    with a strict syntax.

    :arg query: (required) Query string you wish to parse and use for
        search.
    :arg allow_leading_wildcard: If `true`, the wildcard characters `*`
        and `?` are allowed as the first character of the query string.
        Defaults to `True` if omitted.
    :arg analyzer: Analyzer used to convert text in the query string into
        tokens.
    :arg analyze_wildcard: If `true`, the query attempts to analyze
        wildcard terms in the query string.
    :arg auto_generate_synonyms_phrase_query: If `true`, match phrase
        queries are automatically created for multi-term synonyms.
        Defaults to `True` if omitted.
    :arg default_field: Default field to search if no field is provided in
        the query string. Supports wildcards (`*`). Defaults to the
        `index.query.default_field` index setting, which has a default
        value of `*`.
    :arg default_operator: Default boolean logic used to interpret text in
        the query string if no operators are specified. Defaults to `'or'`
        if omitted.
    :arg enable_position_increments: If `true`, enable position increments
        in queries constructed from a `query_string` search. Defaults to
        `True` if omitted.
    :arg escape:
    :arg fields: Array of fields to search. Supports wildcards (`*`).
    :arg fuzziness: Maximum edit distance allowed for fuzzy matching.
    :arg fuzzy_max_expansions: Maximum number of terms to which the query
        expands for fuzzy matching. Defaults to `50` if omitted.
    :arg fuzzy_prefix_length: Number of beginning characters left
        unchanged for fuzzy matching.
    :arg fuzzy_rewrite: Method used to rewrite the query.
    :arg fuzzy_transpositions: If `true`, edits for fuzzy matching include
        transpositions of two adjacent characters (for example, `ab` to
        `ba`). Defaults to `True` if omitted.
    :arg lenient: If `true`, format-based errors, such as providing a text
        value for a numeric field, are ignored.
    :arg max_determinized_states: Maximum number of automaton states
        required for the query. Defaults to `10000` if omitted.
    :arg minimum_should_match: Minimum number of clauses that must match
        for a document to be returned.
    :arg phrase_slop: Maximum number of positions allowed between matching
        tokens for phrases.
    :arg quote_analyzer: Analyzer used to convert quoted text in the query
        string into tokens. For quoted text, this parameter overrides the
        analyzer specified in the `analyzer` parameter.
    :arg quote_field_suffix: Suffix appended to quoted text in the query
        string. You can use this suffix to use a different analysis method
        for exact matches.
    :arg rewrite: Method used to rewrite the query.
    :arg tie_breaker: How to combine the queries generated from the
        individual search terms in the resulting `dis_max` query.
    :arg time_zone: Coordinated Universal Time (UTC) offset or IANA time
        zone used to convert date values in the query string to UTC.
    :arg type: Determines how the query matches and scores documents.
        Defaults to `'best_fields'` if omitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "query_string"

    def __init__(
        self,
        *,
        query: Union[str, "DefaultType"] = DEFAULT,
        allow_leading_wildcard: Union[bool, "DefaultType"] = DEFAULT,
        analyzer: Union[str, "DefaultType"] = DEFAULT,
        analyze_wildcard: Union[bool, "DefaultType"] = DEFAULT,
        auto_generate_synonyms_phrase_query: Union[bool, "DefaultType"] = DEFAULT,
        default_field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        default_operator: Union[Literal["and", "or"], "DefaultType"] = DEFAULT,
        enable_position_increments: Union[bool, "DefaultType"] = DEFAULT,
        escape: Union[bool, "DefaultType"] = DEFAULT,
        fields: Union[
            Sequence[Union[str, "InstrumentedField"]], "DefaultType"
        ] = DEFAULT,
        fuzziness: Union[str, int, "DefaultType"] = DEFAULT,
        fuzzy_max_expansions: Union[int, "DefaultType"] = DEFAULT,
        fuzzy_prefix_length: Union[int, "DefaultType"] = DEFAULT,
        fuzzy_rewrite: Union[str, "DefaultType"] = DEFAULT,
        fuzzy_transpositions: Union[bool, "DefaultType"] = DEFAULT,
        lenient: Union[bool, "DefaultType"] = DEFAULT,
        max_determinized_states: Union[int, "DefaultType"] = DEFAULT,
        minimum_should_match: Union[int, str, "DefaultType"] = DEFAULT,
        phrase_slop: Union[float, "DefaultType"] = DEFAULT,
        quote_analyzer: Union[str, "DefaultType"] = DEFAULT,
        quote_field_suffix: Union[str, "DefaultType"] = DEFAULT,
        rewrite: Union[str, "DefaultType"] = DEFAULT,
        tie_breaker: Union[float, "DefaultType"] = DEFAULT,
        time_zone: Union[str, "DefaultType"] = DEFAULT,
        type: Union[
            Literal[
                "best_fields",
                "most_fields",
                "cross_fields",
                "phrase",
                "phrase_prefix",
                "bool_prefix",
            ],
            "DefaultType",
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            query=query,
            allow_leading_wildcard=allow_leading_wildcard,
            analyzer=analyzer,
            analyze_wildcard=analyze_wildcard,
            auto_generate_synonyms_phrase_query=auto_generate_synonyms_phrase_query,
            default_field=default_field,
            default_operator=default_operator,
            enable_position_increments=enable_position_increments,
            escape=escape,
            fields=fields,
            fuzziness=fuzziness,
            fuzzy_max_expansions=fuzzy_max_expansions,
            fuzzy_prefix_length=fuzzy_prefix_length,
            fuzzy_rewrite=fuzzy_rewrite,
            fuzzy_transpositions=fuzzy_transpositions,
            lenient=lenient,
            max_determinized_states=max_determinized_states,
            minimum_should_match=minimum_should_match,
            phrase_slop=phrase_slop,
            quote_analyzer=quote_analyzer,
            quote_field_suffix=quote_field_suffix,
            rewrite=rewrite,
            tie_breaker=tie_breaker,
            time_zone=time_zone,
            type=type,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Range(Query):
    """
    Returns documents that contain terms within a provided range.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "range"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["wrappers.Range[Any]", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class RankFeature(Query):
    """
    Boosts the relevance score of documents based on the numeric value of
    a `rank_feature` or `rank_features` field.

    :arg field: (required) `rank_feature` or `rank_features` field used to
        boost relevance scores.
    :arg saturation: Saturation function used to boost relevance scores
        based on the value of the rank feature `field`.
    :arg log: Logarithmic function used to boost relevance scores based on
        the value of the rank feature `field`.
    :arg linear: Linear function used to boost relevance scores based on
        the value of the rank feature `field`.
    :arg sigmoid: Sigmoid function used to boost relevance scores based on
        the value of the rank feature `field`.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "rank_feature"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        saturation: Union[
            "types.RankFeatureFunctionSaturation", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        log: Union[
            "types.RankFeatureFunctionLogarithm", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        linear: Union[
            "types.RankFeatureFunctionLinear", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        sigmoid: Union[
            "types.RankFeatureFunctionSigmoid", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            saturation=saturation,
            log=log,
            linear=linear,
            sigmoid=sigmoid,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Regexp(Query):
    """
    Returns documents that contain terms matching a regular expression.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "regexp"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.RegexpQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class Rule(Query):
    """
    :arg organic: (required)
    :arg match_criteria: (required)
    :arg ruleset_ids:
    :arg ruleset_id:
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "rule"
    _param_defs = {
        "organic": {"type": "query"},
    }

    def __init__(
        self,
        *,
        organic: Union[Query, "DefaultType"] = DEFAULT,
        match_criteria: Any = DEFAULT,
        ruleset_ids: Union[str, Sequence[str], "DefaultType"] = DEFAULT,
        ruleset_id: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            organic=organic,
            match_criteria=match_criteria,
            ruleset_ids=ruleset_ids,
            ruleset_id=ruleset_id,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Script(Query):
    """
    Filters documents based on a provided script. The script query is
    typically used in a filter context.

    :arg script: (required) Contains a script to run as a query. This
        script must return a boolean value, `true` or `false`.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "script"

    def __init__(
        self,
        *,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(script=script, boost=boost, _name=_name, **kwargs)


class ScriptScore(Query):
    """
    Uses a script to provide a custom score for returned documents.

    :arg query: (required) Query used to return documents.
    :arg script: (required) Script used to compute the score of documents
        returned by the query. Important: final relevance scores from the
        `script_score` query cannot be negative.
    :arg min_score: Documents with a score lower than this floating point
        number are excluded from the search results.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "script_score"
    _param_defs = {
        "query": {"type": "query"},
    }

    def __init__(
        self,
        *,
        query: Union[Query, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        min_score: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            query=query,
            script=script,
            min_score=min_score,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Semantic(Query):
    """
    A semantic query to semantic_text field types

    :arg field: (required) The field to query, which must be a
        semantic_text field type
    :arg query: (required) The query text
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "semantic"

    def __init__(
        self,
        *,
        field: Union[str, "DefaultType"] = DEFAULT,
        query: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(field=field, query=query, boost=boost, _name=_name, **kwargs)


class Shape(Query):
    """
    Queries documents that contain fields indexed using the `shape` type.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg ignore_unmapped: When set to `true` the query ignores an unmapped
        field and will not match any documents.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "shape"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.ShapeFieldQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        *,
        ignore_unmapped: Union[bool, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(
            ignore_unmapped=ignore_unmapped, boost=boost, _name=_name, **kwargs
        )


class SimpleQueryString(Query):
    """
    Returns documents based on a provided query string, using a parser
    with a limited but fault-tolerant syntax.

    :arg query: (required) Query string in the simple query string syntax
        you wish to parse and use for search.
    :arg analyzer: Analyzer used to convert text in the query string into
        tokens.
    :arg analyze_wildcard: If `true`, the query attempts to analyze
        wildcard terms in the query string.
    :arg auto_generate_synonyms_phrase_query: If `true`, the parser
        creates a match_phrase query for each multi-position token.
        Defaults to `True` if omitted.
    :arg default_operator: Default boolean logic used to interpret text in
        the query string if no operators are specified. Defaults to `'or'`
        if omitted.
    :arg fields: Array of fields you wish to search. Accepts wildcard
        expressions. You also can boost relevance scores for matches to
        particular fields using a caret (`^`) notation. Defaults to the
        `index.query.default_field index` setting, which has a default
        value of `*`.
    :arg flags: List of enabled operators for the simple query string
        syntax. Defaults to `ALL` if omitted.
    :arg fuzzy_max_expansions: Maximum number of terms to which the query
        expands for fuzzy matching. Defaults to `50` if omitted.
    :arg fuzzy_prefix_length: Number of beginning characters left
        unchanged for fuzzy matching.
    :arg fuzzy_transpositions: If `true`, edits for fuzzy matching include
        transpositions of two adjacent characters (for example, `ab` to
        `ba`).
    :arg lenient: If `true`, format-based errors, such as providing a text
        value for a numeric field, are ignored.
    :arg minimum_should_match: Minimum number of clauses that must match
        for a document to be returned.
    :arg quote_field_suffix: Suffix appended to quoted text in the query
        string.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "simple_query_string"

    def __init__(
        self,
        *,
        query: Union[str, "DefaultType"] = DEFAULT,
        analyzer: Union[str, "DefaultType"] = DEFAULT,
        analyze_wildcard: Union[bool, "DefaultType"] = DEFAULT,
        auto_generate_synonyms_phrase_query: Union[bool, "DefaultType"] = DEFAULT,
        default_operator: Union[Literal["and", "or"], "DefaultType"] = DEFAULT,
        fields: Union[
            Sequence[Union[str, "InstrumentedField"]], "DefaultType"
        ] = DEFAULT,
        flags: Union[
            "types.PipeSeparatedFlags", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        fuzzy_max_expansions: Union[int, "DefaultType"] = DEFAULT,
        fuzzy_prefix_length: Union[int, "DefaultType"] = DEFAULT,
        fuzzy_transpositions: Union[bool, "DefaultType"] = DEFAULT,
        lenient: Union[bool, "DefaultType"] = DEFAULT,
        minimum_should_match: Union[int, str, "DefaultType"] = DEFAULT,
        quote_field_suffix: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            query=query,
            analyzer=analyzer,
            analyze_wildcard=analyze_wildcard,
            auto_generate_synonyms_phrase_query=auto_generate_synonyms_phrase_query,
            default_operator=default_operator,
            fields=fields,
            flags=flags,
            fuzzy_max_expansions=fuzzy_max_expansions,
            fuzzy_prefix_length=fuzzy_prefix_length,
            fuzzy_transpositions=fuzzy_transpositions,
            lenient=lenient,
            minimum_should_match=minimum_should_match,
            quote_field_suffix=quote_field_suffix,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class SpanContaining(Query):
    """
    Returns matches which enclose another span query.

    :arg big: (required) Can be any span query. Matching spans from `big`
        that contain matches from `little` are returned.
    :arg little: (required) Can be any span query. Matching spans from
        `big` that contain matches from `little` are returned.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_containing"

    def __init__(
        self,
        *,
        big: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        little: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(big=big, little=little, boost=boost, _name=_name, **kwargs)


class SpanFieldMasking(Query):
    """
    Wrapper to allow span queries to participate in composite single-field
    span queries by _lying_ about their search field.

    :arg field: (required)
    :arg query: (required)
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_field_masking"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        query: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(field=field, query=query, boost=boost, _name=_name, **kwargs)


class SpanFirst(Query):
    """
    Matches spans near the beginning of a field.

    :arg end: (required) Controls the maximum end position permitted in a
        match.
    :arg match: (required) Can be any other span type query.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_first"

    def __init__(
        self,
        *,
        end: Union[int, "DefaultType"] = DEFAULT,
        match: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(end=end, match=match, boost=boost, _name=_name, **kwargs)


class SpanMulti(Query):
    """
    Allows you to wrap a multi term query (one of `wildcard`, `fuzzy`,
    `prefix`, `range`, or `regexp` query) as a `span` query, so it can be
    nested.

    :arg match: (required) Should be a multi term query (one of
        `wildcard`, `fuzzy`, `prefix`, `range`, or `regexp` query).
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_multi"
    _param_defs = {
        "match": {"type": "query"},
    }

    def __init__(
        self,
        *,
        match: Union[Query, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(match=match, boost=boost, _name=_name, **kwargs)


class SpanNear(Query):
    """
    Matches spans which are near one another. You can specify `slop`, the
    maximum number of intervening unmatched positions, as well as whether
    matches are required to be in-order.

    :arg clauses: (required) Array of one or more other span type queries.
    :arg in_order: Controls whether matches are required to be in-order.
    :arg slop: Controls the maximum number of intervening unmatched
        positions permitted.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_near"

    def __init__(
        self,
        *,
        clauses: Union[
            Sequence["types.SpanQuery"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        in_order: Union[bool, "DefaultType"] = DEFAULT,
        slop: Union[int, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            clauses=clauses,
            in_order=in_order,
            slop=slop,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class SpanNot(Query):
    """
    Removes matches which overlap with another span query or which are
    within x tokens before (controlled by the parameter `pre`) or y tokens
    after (controlled by the parameter `post`) another span query.

    :arg exclude: (required) Span query whose matches must not overlap
        those returned.
    :arg include: (required) Span query whose matches are filtered.
    :arg dist: The number of tokens from within the include span that
        can’t have overlap with the exclude span. Equivalent to setting
        both `pre` and `post`.
    :arg post: The number of tokens after the include span that can’t have
        overlap with the exclude span.
    :arg pre: The number of tokens before the include span that can’t have
        overlap with the exclude span.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_not"

    def __init__(
        self,
        *,
        exclude: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        include: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        dist: Union[int, "DefaultType"] = DEFAULT,
        post: Union[int, "DefaultType"] = DEFAULT,
        pre: Union[int, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            exclude=exclude,
            include=include,
            dist=dist,
            post=post,
            pre=pre,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class SpanOr(Query):
    """
    Matches the union of its span clauses.

    :arg clauses: (required) Array of one or more other span type queries.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_or"

    def __init__(
        self,
        *,
        clauses: Union[
            Sequence["types.SpanQuery"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(clauses=clauses, boost=boost, _name=_name, **kwargs)


class SpanTerm(Query):
    """
    Matches spans containing a term.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "span_term"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.SpanTermQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class SpanWithin(Query):
    """
    Returns matches which are enclosed inside another span query.

    :arg big: (required) Can be any span query. Matching spans from
        `little` that are enclosed within `big` are returned.
    :arg little: (required) Can be any span query. Matching spans from
        `little` that are enclosed within `big` are returned.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "span_within"

    def __init__(
        self,
        *,
        big: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        little: Union["types.SpanQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(big=big, little=little, boost=boost, _name=_name, **kwargs)


class SparseVector(Query):
    """
    Using input query vectors or a natural language processing model to
    convert a query into a list of token-weight pairs, queries against a
    sparse vector field.

    :arg field: (required) The name of the field that contains the token-
        weight pairs to be searched against. This field must be a mapped
        sparse_vector field.
    :arg query_vector: Dictionary of precomputed sparse vectors and their
        associated weights. Only one of inference_id or query_vector may
        be supplied in a request.
    :arg inference_id: The inference ID to use to convert the query text
        into token-weight pairs. It must be the same inference ID that was
        used to create the tokens from the input text. Only one of
        inference_id and query_vector is allowed. If inference_id is
        specified, query must also be specified. Only one of inference_id
        or query_vector may be supplied in a request.
    :arg query: The query text you want to use for search. If inference_id
        is specified, query must also be specified.
    :arg prune: Whether to perform pruning, omitting the non-significant
        tokens from the query to improve query performance. If prune is
        true but the pruning_config is not specified, pruning will occur
        but default values will be used. Default: false
    :arg pruning_config: Optional pruning configuration. If enabled, this
        will omit non-significant tokens from the query in order to
        improve query performance. This is only used if prune is set to
        true. If prune is set to true but pruning_config is not specified,
        default values will be used.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "sparse_vector"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        query_vector: Union[Mapping[str, float], "DefaultType"] = DEFAULT,
        inference_id: Union[str, "DefaultType"] = DEFAULT,
        query: Union[str, "DefaultType"] = DEFAULT,
        prune: Union[bool, "DefaultType"] = DEFAULT,
        pruning_config: Union[
            "types.TokenPruningConfig", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            query_vector=query_vector,
            inference_id=inference_id,
            query=query,
            prune=prune,
            pruning_config=pruning_config,
            boost=boost,
            _name=_name,
            **kwargs,
        )


class Term(Query):
    """
    Returns documents that contain an exact term in a provided field. To
    return a document, the query term must exactly match the queried
    field's value, including whitespace and capitalization.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "term"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.TermQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class Terms(Query):
    """
    Returns documents that contain one or more exact terms in a provided
    field. To return a document, one or more terms must exactly match a
    field value, including whitespace and capitalization.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "terms"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            Sequence[Union[int, float, str, bool, None]],
            "types.TermsLookup",
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        *,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(boost=boost, _name=_name, **kwargs)

    def _setattr(self, name: str, value: Any) -> None:
        # here we convert any iterables that are not strings to lists
        if hasattr(value, "__iter__") and not isinstance(value, (str, list, dict)):
            value = list(value)
        super()._setattr(name, value)


class TermsSet(Query):
    """
    Returns documents that contain a minimum number of exact terms in a
    provided field. To return a document, a required number of terms must
    exactly match the field values, including whitespace and
    capitalization.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "terms_set"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.TermsSetQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class TextExpansion(Query):
    """
    Uses a natural language processing model to convert the query text
    into a list of token-weight pairs which are then used in a query
    against a sparse vector or rank features field.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "text_expansion"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.TextExpansionQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class WeightedTokens(Query):
    """
    Supports returning text_expansion query results by sending in
    precomputed tokens with the query.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "weighted_tokens"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            "types.WeightedTokensQuery", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class Wildcard(Query):
    """
    Returns documents that contain terms matching a wildcard pattern.

    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    """

    name = "wildcard"

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["types.WildcardQuery", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        super().__init__(**kwargs)


class Wrapper(Query):
    """
    A query that accepts any other query as base64 encoded string.

    :arg query: (required) A base64 encoded query. The binary data format
        can be any of JSON, YAML, CBOR or SMILE encodings
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "wrapper"

    def __init__(
        self,
        *,
        query: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(query=query, boost=boost, _name=_name, **kwargs)


class Type(Query):
    """
    :arg value: (required)
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    name = "type"

    def __init__(
        self,
        *,
        value: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        _name: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(value=value, boost=boost, _name=_name, **kwargs)
