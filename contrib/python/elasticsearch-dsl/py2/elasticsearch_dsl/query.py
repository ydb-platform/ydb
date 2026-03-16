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

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:
    import collections as collections_abc

from itertools import chain

# 'SF' looks unused but the test suite assumes it's available
# from this module so others are liable to do so as well.
from .function import SF  # noqa: F401
from .function import ScoreFunction
from .utils import DslBase


def Q(name_or_query="match_all", **params):
    # {"match": {"title": "python"}}
    if isinstance(name_or_query, collections_abc.Mapping):
        if params:
            raise ValueError("Q() cannot accept parameters when passing in a dict.")
        if len(name_or_query) != 1:
            raise ValueError(
                'Q() can only accept dict with a single query ({"match": {...}}). '
                "Instead it got (%r)" % name_or_query
            )
        name, params = name_or_query.copy().popitem()
        return Query.get_dsl_class(name)(_expand__to_dot=False, **params)

    # MatchAll()
    if isinstance(name_or_query, Query):
        if params:
            raise ValueError(
                "Q() cannot accept parameters when passing in a Query object."
            )
        return name_or_query

    # s.query = Q('filtered', query=s.query)
    if hasattr(name_or_query, "_proxied"):
        return name_or_query._proxied

    # "match", title="python"
    return Query.get_dsl_class(name_or_query)(**params)


class Query(DslBase):
    _type_name = "query"
    _type_shortcut = staticmethod(Q)
    name = None

    def __add__(self, other):
        # make sure we give queries that know how to combine themselves
        # preference
        if hasattr(other, "__radd__"):
            return other.__radd__(self)
        return Bool(must=[self, other])

    def __invert__(self):
        return Bool(must_not=[self])

    def __or__(self, other):
        # make sure we give queries that know how to combine themselves
        # preference
        if hasattr(other, "__ror__"):
            return other.__ror__(self)
        return Bool(should=[self, other])

    def __and__(self, other):
        # make sure we give queries that know how to combine themselves
        # preference
        if hasattr(other, "__rand__"):
            return other.__rand__(self)
        return Bool(must=[self, other])


class MatchAll(Query):
    name = "match_all"

    def __add__(self, other):
        return other._clone()

    __and__ = __rand__ = __radd__ = __add__

    def __or__(self, other):
        return self

    __ror__ = __or__

    def __invert__(self):
        return MatchNone()


EMPTY_QUERY = MatchAll()


class MatchNone(Query):
    name = "match_none"

    def __add__(self, other):
        return self

    __and__ = __rand__ = __radd__ = __add__

    def __or__(self, other):
        return other._clone()

    __ror__ = __or__

    def __invert__(self):
        return MatchAll()


class Bool(Query):
    name = "bool"
    _param_defs = {
        "must": {"type": "query", "multi": True},
        "should": {"type": "query", "multi": True},
        "must_not": {"type": "query", "multi": True},
        "filter": {"type": "query", "multi": True},
    }

    def __add__(self, other):
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

    def __or__(self, other):
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
    def _min_should_match(self):
        return getattr(
            self,
            "minimum_should_match",
            0 if not self.should or (self.must or self.filter) else 1,
        )

    def __invert__(self):
        # Because an empty Bool query is treated like
        # MatchAll the inverse should be MatchNone
        if not any(chain(self.must, self.filter, self.should, self.must_not)):
            return MatchNone()

        negations = []
        for q in chain(self.must, self.filter):
            negations.append(~q)

        for q in self.must_not:
            negations.append(q)

        if self.should and self._min_should_match:
            negations.append(Bool(must_not=self.should[:]))

        if len(negations) == 1:
            return negations[0]
        return Bool(should=negations)

    def __and__(self, other):
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
                # TODO: percentages will fail here
                min_should_match = qx._min_should_match
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


class FunctionScore(Query):
    name = "function_score"
    _param_defs = {
        "query": {"type": "query"},
        "filter": {"type": "query"},
        "functions": {"type": "score_function", "multi": True},
    }

    def __init__(self, **kwargs):
        if "functions" in kwargs:
            pass
        else:
            fns = kwargs["functions"] = []
            for name in ScoreFunction._classes:
                if name in kwargs:
                    fns.append({name: kwargs.pop(name)})
        super(FunctionScore, self).__init__(**kwargs)


# compound queries
class Boosting(Query):
    name = "boosting"
    _param_defs = {"positive": {"type": "query"}, "negative": {"type": "query"}}


class ConstantScore(Query):
    name = "constant_score"
    _param_defs = {"query": {"type": "query"}, "filter": {"type": "query"}}


class DisMax(Query):
    name = "dis_max"
    _param_defs = {"queries": {"type": "query", "multi": True}}


class Filtered(Query):
    name = "filtered"
    _param_defs = {"query": {"type": "query"}, "filter": {"type": "query"}}


class Indices(Query):
    name = "indices"
    _param_defs = {"query": {"type": "query"}, "no_match_query": {"type": "query"}}


class Percolate(Query):
    name = "percolate"


# relationship queries
class Nested(Query):
    name = "nested"
    _param_defs = {"query": {"type": "query"}}


class HasChild(Query):
    name = "has_child"
    _param_defs = {"query": {"type": "query"}}


class HasParent(Query):
    name = "has_parent"
    _param_defs = {"query": {"type": "query"}}


class TopChildren(Query):
    name = "top_children"
    _param_defs = {"query": {"type": "query"}}


# compount span queries
class SpanFirst(Query):
    name = "span_first"
    _param_defs = {"match": {"type": "query"}}


class SpanMulti(Query):
    name = "span_multi"
    _param_defs = {"match": {"type": "query"}}


class SpanNear(Query):
    name = "span_near"
    _param_defs = {"clauses": {"type": "query", "multi": True}}


class SpanNot(Query):
    name = "span_not"
    _param_defs = {"exclude": {"type": "query"}, "include": {"type": "query"}}


class SpanOr(Query):
    name = "span_or"
    _param_defs = {"clauses": {"type": "query", "multi": True}}


class FieldMaskingSpan(Query):
    name = "field_masking_span"
    _param_defs = {"query": {"type": "query"}}


class SpanContaining(Query):
    name = "span_containing"
    _param_defs = {"little": {"type": "query"}, "big": {"type": "query"}}


# Original implementation contained
# a typo: remove in v8.0.
SpanContainining = SpanContaining


class SpanWithin(Query):
    name = "span_within"
    _param_defs = {"little": {"type": "query"}, "big": {"type": "query"}}


# core queries
class Common(Query):
    name = "common"


class Fuzzy(Query):
    name = "fuzzy"


class FuzzyLikeThis(Query):
    name = "fuzzy_like_this"


class FuzzyLikeThisField(Query):
    name = "fuzzy_like_this_field"


class RankFeature(Query):
    name = "rank_feature"


class DistanceFeature(Query):
    name = "distance_feature"


class GeoBoundingBox(Query):
    name = "geo_bounding_box"


class GeoDistance(Query):
    name = "geo_distance"


class GeoDistanceRange(Query):
    name = "geo_distance_range"


class GeoPolygon(Query):
    name = "geo_polygon"


class GeoShape(Query):
    name = "geo_shape"


class GeohashCell(Query):
    name = "geohash_cell"


class Ids(Query):
    name = "ids"


class Intervals(Query):
    name = "intervals"


class Limit(Query):
    name = "limit"


class Match(Query):
    name = "match"


class MatchPhrase(Query):
    name = "match_phrase"


class MatchPhrasePrefix(Query):
    name = "match_phrase_prefix"


class MatchBoolPrefix(Query):
    name = "match_bool_prefix"


class Exists(Query):
    name = "exists"


class MoreLikeThis(Query):
    name = "more_like_this"


class MoreLikeThisField(Query):
    name = "more_like_this_field"


class MultiMatch(Query):
    name = "multi_match"


class Prefix(Query):
    name = "prefix"


class QueryString(Query):
    name = "query_string"


class Range(Query):
    name = "range"


class Regexp(Query):
    name = "regexp"


class Shape(Query):
    name = "shape"


class SimpleQueryString(Query):
    name = "simple_query_string"


class SpanTerm(Query):
    name = "span_term"


class Template(Query):
    name = "template"


class Term(Query):
    name = "term"


class Terms(Query):
    name = "terms"


class TermsSet(Query):
    name = "terms_set"


class Wildcard(Query):
    name = "wildcard"


class Script(Query):
    name = "script"


class ScriptScore(Query):
    name = "script_score"
    _param_defs = {"query": {"type": "query"}}


class Type(Query):
    name = "type"


class ParentId(Query):
    name = "parent_id"


class Wrapper(Query):
    name = "wrapper"
