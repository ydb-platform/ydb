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

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from ..utils import _R, AttrDict, AttrList, _wrap
from .hit import Hit, HitMeta

if TYPE_CHECKING:
    from .. import types
    from ..aggs import Agg
    from ..faceted_search_base import FacetedSearchBase
    from ..search_base import Request, SearchBase
    from ..update_by_query_base import UpdateByQueryBase

__all__ = [
    "Response",
    "AggResponse",
    "UpdateByQueryResponse",
    "Hit",
    "HitMeta",
    "AggregateResponseType",
]


class Response(AttrDict[Any], Generic[_R]):
    """An Elasticsearch search response.

    :arg took: (required) The number of milliseconds it took Elasticsearch
        to run the request. This value is calculated by measuring the time
        elapsed between receipt of a request on the coordinating node and
        the time at which the coordinating node is ready to send the
        response. It includes:  * Communication time between the
        coordinating node and data nodes * Time the request spends in the
        search thread pool, queued for execution * Actual run time  It
        does not include:  * Time needed to send the request to
        Elasticsearch * Time needed to serialize the JSON response * Time
        needed to send the response to a client
    :arg timed_out: (required) If `true`, the request timed out before
        completion; returned results may be partial or empty.
    :arg _shards: (required) A count of shards used for the request.
    :arg hits: search results
    :arg aggregations: aggregation results
    :arg _clusters:
    :arg fields:
    :arg max_score:
    :arg num_reduce_phases:
    :arg profile:
    :arg pit_id:
    :arg _scroll_id: The identifier for the search and its search context.
        You can use this scroll ID with the scroll API to retrieve the
        next batch of search results for the request. This property is
        returned only if the `scroll` query parameter is specified in the
        request.
    :arg suggest:
    :arg terminated_early:
    """

    _search: "SearchBase[_R]"
    _faceted_search: "FacetedSearchBase[_R]"
    _doc_class: Optional[_R]
    _hits: List[_R]

    took: int
    timed_out: bool
    _shards: "types.ShardStatistics"
    _clusters: "types.ClusterStatistics"
    fields: Mapping[str, Any]
    max_score: float
    num_reduce_phases: int
    profile: "types.Profile"
    pit_id: str
    _scroll_id: str
    suggest: Mapping[
        str,
        Sequence[
            Union["types.CompletionSuggest", "types.PhraseSuggest", "types.TermSuggest"]
        ],
    ]
    terminated_early: bool

    def __init__(
        self,
        search: "Request[_R]",
        response: Dict[str, Any],
        doc_class: Optional[_R] = None,
    ):
        super(AttrDict, self).__setattr__("_search", search)
        super(AttrDict, self).__setattr__("_doc_class", doc_class)
        super().__init__(response)

    def __iter__(self) -> Iterator[_R]:  # type: ignore[override]
        return iter(self.hits)

    def __getitem__(self, key: Union[slice, int, str]) -> Any:
        if isinstance(key, (slice, int)):
            # for slicing etc
            return self.hits[key]
        return super().__getitem__(key)

    def __nonzero__(self) -> bool:
        return bool(self.hits)

    __bool__ = __nonzero__

    def __repr__(self) -> str:
        return "<Response: %r>" % (self.hits or self.aggregations)

    def __len__(self) -> int:
        return len(self.hits)

    def __getstate__(self) -> Tuple[Dict[str, Any], "Request[_R]", Optional[_R]]:  # type: ignore[override]
        return self._d_, self._search, self._doc_class

    def __setstate__(
        self, state: Tuple[Dict[str, Any], "Request[_R]", Optional[_R]]  # type: ignore[override]
    ) -> None:
        super(AttrDict, self).__setattr__("_d_", state[0])
        super(AttrDict, self).__setattr__("_search", state[1])
        super(AttrDict, self).__setattr__("_doc_class", state[2])

    def success(self) -> bool:
        return self._shards.total == self._shards.successful and not self.timed_out

    @property
    def hits(self) -> List[_R]:
        if not hasattr(self, "_hits"):
            h = cast(AttrDict[Any], self._d_["hits"])

            try:
                hits = AttrList(list(map(self._search._get_result, h["hits"])))
            except AttributeError as e:
                # avoid raising AttributeError since it will be hidden by the property
                raise TypeError("Could not parse hits.", e)

            # avoid assigning _hits into self._d_
            super(AttrDict, self).__setattr__("_hits", hits)
            for k in h:
                setattr(self._hits, k, _wrap(h[k]))
        return self._hits

    @property
    def aggregations(self) -> "AggResponse[_R]":
        return self.aggs

    @property
    def aggs(self) -> "AggResponse[_R]":
        if not hasattr(self, "_aggs"):
            aggs = AggResponse[_R](
                cast("Agg[_R]", self._search.aggs),
                self._search,
                cast(Dict[str, Any], self._d_.get("aggregations", {})),
            )

            # avoid assigning _aggs into self._d_
            super(AttrDict, self).__setattr__("_aggs", aggs)
        return cast("AggResponse[_R]", self._aggs)

    def search_after(self) -> "SearchBase[_R]":
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
                s = r.search_after()

        Note that the ``search_after`` option requires the search to have an
        explicit ``sort`` order.
        """
        if len(self.hits) == 0:
            raise ValueError("Cannot use search_after when there are no search results")
        if not hasattr(self.hits[-1].meta, "sort"):  # type: ignore[attr-defined]
            raise ValueError("Cannot use search_after when results are not sorted")
        return self._search.extra(search_after=self.hits[-1].meta.sort)  # type: ignore[attr-defined]


AggregateResponseType = Union[
    "types.CardinalityAggregate",
    "types.HdrPercentilesAggregate",
    "types.HdrPercentileRanksAggregate",
    "types.TDigestPercentilesAggregate",
    "types.TDigestPercentileRanksAggregate",
    "types.PercentilesBucketAggregate",
    "types.MedianAbsoluteDeviationAggregate",
    "types.MinAggregate",
    "types.MaxAggregate",
    "types.SumAggregate",
    "types.AvgAggregate",
    "types.WeightedAvgAggregate",
    "types.ValueCountAggregate",
    "types.SimpleValueAggregate",
    "types.DerivativeAggregate",
    "types.BucketMetricValueAggregate",
    "types.StatsAggregate",
    "types.StatsBucketAggregate",
    "types.ExtendedStatsAggregate",
    "types.ExtendedStatsBucketAggregate",
    "types.GeoBoundsAggregate",
    "types.GeoCentroidAggregate",
    "types.HistogramAggregate",
    "types.DateHistogramAggregate",
    "types.AutoDateHistogramAggregate",
    "types.VariableWidthHistogramAggregate",
    "types.StringTermsAggregate",
    "types.LongTermsAggregate",
    "types.DoubleTermsAggregate",
    "types.UnmappedTermsAggregate",
    "types.LongRareTermsAggregate",
    "types.StringRareTermsAggregate",
    "types.UnmappedRareTermsAggregate",
    "types.MultiTermsAggregate",
    "types.MissingAggregate",
    "types.NestedAggregate",
    "types.ReverseNestedAggregate",
    "types.GlobalAggregate",
    "types.FilterAggregate",
    "types.ChildrenAggregate",
    "types.ParentAggregate",
    "types.SamplerAggregate",
    "types.UnmappedSamplerAggregate",
    "types.GeoHashGridAggregate",
    "types.GeoTileGridAggregate",
    "types.GeoHexGridAggregate",
    "types.RangeAggregate",
    "types.DateRangeAggregate",
    "types.GeoDistanceAggregate",
    "types.IpRangeAggregate",
    "types.IpPrefixAggregate",
    "types.FiltersAggregate",
    "types.AdjacencyMatrixAggregate",
    "types.SignificantLongTermsAggregate",
    "types.SignificantStringTermsAggregate",
    "types.UnmappedSignificantTermsAggregate",
    "types.CompositeAggregate",
    "types.FrequentItemSetsAggregate",
    "types.TimeSeriesAggregate",
    "types.ScriptedMetricAggregate",
    "types.TopHitsAggregate",
    "types.InferenceAggregate",
    "types.StringStatsAggregate",
    "types.BoxPlotAggregate",
    "types.TopMetricsAggregate",
    "types.TTestAggregate",
    "types.RateAggregate",
    "types.CumulativeCardinalityAggregate",
    "types.MatrixStatsAggregate",
    "types.GeoLineAggregate",
]


class AggResponse(AttrDict[Any], Generic[_R]):
    """An Elasticsearch aggregation response."""

    _meta: Dict[str, Any]

    def __init__(self, aggs: "Agg[_R]", search: "Request[_R]", data: Dict[str, Any]):
        super(AttrDict, self).__setattr__("_meta", {"search": search, "aggs": aggs})
        super().__init__(data)

    def __getitem__(self, attr_name: str) -> AggregateResponseType:
        if attr_name in self._meta["aggs"]:
            # don't do self._meta['aggs'][attr_name] to avoid copying
            agg = self._meta["aggs"].aggs[attr_name]
            return cast(
                AggregateResponseType,
                agg.result(self._meta["search"], self._d_[attr_name]),
            )
        return super().__getitem__(attr_name)  # type: ignore[no-any-return]

    def __iter__(self) -> Iterator[AggregateResponseType]:  # type: ignore[override]
        for name in self._meta["aggs"]:
            yield self[name]


class UpdateByQueryResponse(AttrDict[Any], Generic[_R]):
    """An Elasticsearch update by query response.

    :arg batches: The number of scroll responses pulled back by the update
        by query.
    :arg failures: Array of failures if there were any unrecoverable
        errors during the process. If this is non-empty then the request
        ended because of those failures. Update by query is implemented
        using batches. Any failure causes the entire process to end, but
        all failures in the current batch are collected into the array.
        You can use the `conflicts` option to prevent reindex from ending
        when version conflicts occur.
    :arg noops: The number of documents that were ignored because the
        script used for the update by query returned a noop value for
        `ctx.op`.
    :arg deleted: The number of documents that were successfully deleted.
    :arg requests_per_second: The number of requests per second
        effectively run during the update by query.
    :arg retries: The number of retries attempted by update by query.
        `bulk` is the number of bulk actions retried. `search` is the
        number of search actions retried.
    :arg task:
    :arg timed_out: If true, some requests timed out during the update by
        query.
    :arg took: The number of milliseconds from start to end of the whole
        operation.
    :arg total: The number of documents that were successfully processed.
    :arg updated: The number of documents that were successfully updated.
    :arg version_conflicts: The number of version conflicts that the
        update by query hit.
    :arg throttled:
    :arg throttled_millis: The number of milliseconds the request slept to
        conform to `requests_per_second`.
    :arg throttled_until:
    :arg throttled_until_millis: This field should always be equal to zero
        in an _update_by_query response. It only has meaning when using
        the task API, where it indicates the next time (in milliseconds
        since epoch) a throttled request will be run again in order to
        conform to `requests_per_second`.
    """

    _search: "UpdateByQueryBase[_R]"

    batches: int
    failures: Sequence["types.BulkIndexByScrollFailure"]
    noops: int
    deleted: int
    requests_per_second: float
    retries: "types.Retries"
    task: str
    timed_out: bool
    took: Any
    total: int
    updated: int
    version_conflicts: int
    throttled: Any
    throttled_millis: Any
    throttled_until: Any
    throttled_until_millis: Any

    def __init__(
        self,
        search: "Request[_R]",
        response: Dict[str, Any],
        doc_class: Optional[_R] = None,
    ):
        super(AttrDict, self).__setattr__("_search", search)
        super(AttrDict, self).__setattr__("_doc_class", doc_class)
        super().__init__(response)

    def success(self) -> bool:
        return not self.timed_out and not self.failures
