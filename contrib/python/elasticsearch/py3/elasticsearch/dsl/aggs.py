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
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    cast,
)

from elastic_transport.client_utils import DEFAULT

from . import wrappers
from .query import Query
from .response.aggs import AggResponse, BucketData, FieldBucketData, TopHitsData
from .utils import _R, AttrDict, DslBase

if TYPE_CHECKING:
    from elastic_transport.client_utils import DefaultType

    from . import types
    from .document_base import InstrumentedField
    from .search_base import SearchBase


def A(
    name_or_agg: Union[MutableMapping[str, Any], "Agg[_R]", str],
    filter: Optional[Union[str, "Query"]] = None,
    **params: Any,
) -> "Agg[_R]":
    if filter is not None:
        if name_or_agg != "filter":
            raise ValueError(
                "Aggregation %r doesn't accept positional argument 'filter'."
                % name_or_agg
            )
        params["filter"] = filter

    # {"terms": {"field": "tags"}, "aggs": {...}}
    if isinstance(name_or_agg, collections.abc.MutableMapping):
        if params:
            raise ValueError("A() cannot accept parameters when passing in a dict.")
        # copy to avoid modifying in-place
        agg = deepcopy(name_or_agg)
        # pop out nested aggs
        aggs = agg.pop("aggs", None)
        # pop out meta data
        meta = agg.pop("meta", None)
        # should be {"terms": {"field": "tags"}}
        if len(agg) != 1:
            raise ValueError(
                'A() can only accept dict with an aggregation ({"terms": {...}}). '
                "Instead it got (%r)" % name_or_agg
            )
        agg_type, params = agg.popitem()
        if aggs:
            params = params.copy()
            params["aggs"] = aggs
        if meta:
            params = params.copy()
            params["meta"] = meta
        return Agg[_R].get_dsl_class(agg_type)(_expand__to_dot=False, **params)

    # Terms(...) just return the nested agg
    elif isinstance(name_or_agg, Agg):
        if params:
            raise ValueError(
                "A() cannot accept parameters when passing in an Agg object."
            )
        return name_or_agg

    # "terms", field="tags"
    return Agg[_R].get_dsl_class(name_or_agg)(**params)


class Agg(DslBase, Generic[_R]):
    _type_name = "agg"
    _type_shortcut = staticmethod(A)
    name = ""

    def __contains__(self, key: str) -> bool:
        return False

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if isinstance(d[self.name], dict):
            n = cast(Dict[str, Any], d[self.name])
            if "meta" in n:
                d["meta"] = n.pop("meta")
        return d

    def result(self, search: "SearchBase[_R]", data: Dict[str, Any]) -> AttrDict[Any]:
        return AggResponse[_R](self, search, data)


class AggBase(Generic[_R]):
    aggs: Dict[str, Agg[_R]]
    _base: Agg[_R]
    _params: Dict[str, Any]
    _param_defs: ClassVar[Dict[str, Any]] = {
        "aggs": {"type": "agg", "hash": True},
    }

    def __contains__(self, key: str) -> bool:
        return key in self._params.get("aggs", {})

    def __getitem__(self, agg_name: str) -> Agg[_R]:
        agg = cast(
            Agg[_R], self._params.setdefault("aggs", {})[agg_name]
        )  # propagate KeyError

        # make sure we're not mutating a shared state - whenever accessing a
        # bucket, return a shallow copy of it to be safe
        if isinstance(agg, Bucket):
            agg = A(agg.name, **agg._params)
            # be sure to store the copy so any modifications to it will affect us
            self._params["aggs"][agg_name] = agg

        return agg

    def __setitem__(self, agg_name: str, agg: Agg[_R]) -> None:
        self.aggs[agg_name] = A(agg)

    def __iter__(self) -> Iterable[str]:
        return iter(self.aggs)

    def _agg(
        self,
        bucket: bool,
        name: str,
        agg_type: Union[Dict[str, Any], Agg[_R], str],
        *args: Any,
        **params: Any,
    ) -> Agg[_R]:
        agg = self[name] = A(agg_type, *args, **params)

        # For chaining - when creating new buckets return them...
        if bucket:
            return agg
        # otherwise return self._base so we can keep chaining
        else:
            return self._base

    def metric(
        self,
        name: str,
        agg_type: Union[Dict[str, Any], Agg[_R], str],
        *args: Any,
        **params: Any,
    ) -> Agg[_R]:
        return self._agg(False, name, agg_type, *args, **params)

    def bucket(
        self,
        name: str,
        agg_type: Union[Dict[str, Any], Agg[_R], str],
        *args: Any,
        **params: Any,
    ) -> "Bucket[_R]":
        return cast("Bucket[_R]", self._agg(True, name, agg_type, *args, **params))

    def pipeline(
        self,
        name: str,
        agg_type: Union[Dict[str, Any], Agg[_R], str],
        *args: Any,
        **params: Any,
    ) -> "Pipeline[_R]":
        return cast("Pipeline[_R]", self._agg(False, name, agg_type, *args, **params))

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return BucketData(self, search, data)  # type: ignore[arg-type]


class Bucket(AggBase[_R], Agg[_R]):
    def __init__(self, **params: Any):
        super().__init__(**params)
        # remember self for chaining
        self._base = self

    def to_dict(self) -> Dict[str, Any]:
        d = super(AggBase, self).to_dict()
        if isinstance(d[self.name], dict):
            n = cast(AttrDict[Any], d[self.name])
            if "aggs" in n:
                d["aggs"] = n.pop("aggs")
        return d


class Pipeline(Agg[_R]):
    pass


class AdjacencyMatrix(Bucket[_R]):
    """
    A bucket aggregation returning a form of adjacency matrix. The request
    provides a collection of named filter expressions, similar to the
    `filters` aggregation. Each bucket in the response represents a non-
    empty cell in the matrix of intersecting filters.

    :arg filters: Filters used to create buckets. At least one filter is
        required.
    :arg separator: Separator used to concatenate filter names. Defaults
        to &.
    """

    name = "adjacency_matrix"
    _param_defs = {
        "filters": {"type": "query", "hash": True},
    }

    def __init__(
        self,
        *,
        filters: Union[Mapping[str, Query], "DefaultType"] = DEFAULT,
        separator: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(filters=filters, separator=separator, **kwargs)


class AutoDateHistogram(Bucket[_R]):
    """
    A multi-bucket aggregation similar to the date histogram, except
    instead of providing an interval to use as the width of each bucket, a
    target number of buckets is provided.

    :arg buckets: The target number of buckets. Defaults to `10` if
        omitted.
    :arg field: The field on which to run the aggregation.
    :arg format: The date format used to format `key_as_string` in the
        response. If no `format` is specified, the first date format
        specified in the field mapping is used.
    :arg minimum_interval: The minimum rounding interval. This can make
        the collection process more efficient, as the aggregation will not
        attempt to round at any interval lower than `minimum_interval`.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg offset: Time zone specified as a ISO 8601 UTC offset.
    :arg params:
    :arg script:
    :arg time_zone: Time zone ID.
    """

    name = "auto_date_histogram"

    def __init__(
        self,
        *,
        buckets: Union[int, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        minimum_interval: Union[
            Literal["second", "minute", "hour", "day", "month", "year"], "DefaultType"
        ] = DEFAULT,
        missing: Any = DEFAULT,
        offset: Union[str, "DefaultType"] = DEFAULT,
        params: Union[Mapping[str, Any], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_zone: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            buckets=buckets,
            field=field,
            format=format,
            minimum_interval=minimum_interval,
            missing=missing,
            offset=offset,
            params=params,
            script=script,
            time_zone=time_zone,
            **kwargs,
        )

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return FieldBucketData(self, search, data)


class Avg(Agg[_R]):
    """
    A single-value metrics aggregation that computes the average of
    numeric values that are extracted from the aggregated documents.

    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "avg"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, field=field, missing=missing, script=script, **kwargs
        )


class AvgBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which calculates the mean value of a
    specified metric in a sibling aggregation. The specified metric must
    be numeric and the sibling aggregation must be a multi-bucket
    aggregation.

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "avg_bucket"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class Boxplot(Agg[_R]):
    """
    A metrics aggregation that computes a box plot of numeric values
    extracted from the aggregated documents.

    :arg compression: Limits the maximum number of nodes used by the
        underlying TDigest algorithm to `20 * compression`, enabling
        control of memory usage and approximation error.
    :arg execution_hint: The default implementation of TDigest is
        optimized for performance, scaling to millions or even billions of
        sample values while maintaining acceptable accuracy levels (close
        to 1% relative error for millions of samples in some cases). To
        use an implementation optimized for accuracy, set this parameter
        to high_accuracy instead. Defaults to `default` if omitted.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "boxplot"

    def __init__(
        self,
        *,
        compression: Union[float, "DefaultType"] = DEFAULT,
        execution_hint: Union[
            Literal["default", "high_accuracy"], "DefaultType"
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            compression=compression,
            execution_hint=execution_hint,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class BucketScript(Pipeline[_R]):
    """
    A parent pipeline aggregation which runs a script which can perform
    per bucket computations on metrics in the parent multi-bucket
    aggregation.

    :arg script: The script to run for this aggregation.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "bucket_script"

    def __init__(
        self,
        *,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            script=script,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class BucketSelector(Pipeline[_R]):
    """
    A parent pipeline aggregation which runs a script to determine whether
    the current bucket will be retained in the parent multi-bucket
    aggregation.

    :arg script: The script to run for this aggregation.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "bucket_selector"

    def __init__(
        self,
        *,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            script=script,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class BucketSort(Bucket[_R]):
    """
    A parent pipeline aggregation which sorts the buckets of its parent
    multi-bucket aggregation.

    :arg from: Buckets in positions prior to `from` will be truncated.
    :arg gap_policy: The policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg size: The number of buckets to return. Defaults to all buckets of
        the parent aggregation.
    :arg sort: The list of fields to sort on.
    """

    name = "bucket_sort"

    def __init__(
        self,
        *,
        from_: Union[int, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        sort: Union[
            Union[Union[str, "InstrumentedField"], "types.SortOptions"],
            Sequence[Union[Union[str, "InstrumentedField"], "types.SortOptions"]],
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            from_=from_, gap_policy=gap_policy, size=size, sort=sort, **kwargs
        )


class BucketCountKsTest(Pipeline[_R]):
    """
    A sibling pipeline aggregation which runs a two sample
    Kolmogorov–Smirnov test ("K-S test") against a provided distribution
    and the distribution implied by the documents counts in the configured
    sibling aggregation.

    :arg alternative: A list of string values indicating which K-S test
        alternative to calculate. The valid values are: "greater", "less",
        "two_sided". This parameter is key for determining the K-S
        statistic used when calculating the K-S test. Default value is all
        possible alternative hypotheses.
    :arg fractions: A list of doubles indicating the distribution of the
        samples with which to compare to the `buckets_path` results. In
        typical usage this is the overall proportion of documents in each
        bucket, which is compared with the actual document proportions in
        each bucket from the sibling aggregation counts. The default is to
        assume that overall documents are uniformly distributed on these
        buckets, which they would be if one used equal percentiles of a
        metric to define the bucket end points.
    :arg sampling_method: Indicates the sampling methodology when
        calculating the K-S test. Note, this is sampling of the returned
        values. This determines the cumulative distribution function (CDF)
        points used comparing the two samples. Default is `upper_tail`,
        which emphasizes the upper end of the CDF points. Valid options
        are: `upper_tail`, `uniform`, and `lower_tail`.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "bucket_count_ks_test"

    def __init__(
        self,
        *,
        alternative: Union[Sequence[str], "DefaultType"] = DEFAULT,
        fractions: Union[Sequence[float], "DefaultType"] = DEFAULT,
        sampling_method: Union[str, "DefaultType"] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            alternative=alternative,
            fractions=fractions,
            sampling_method=sampling_method,
            buckets_path=buckets_path,
            **kwargs,
        )


class BucketCorrelation(Pipeline[_R]):
    """
    A sibling pipeline aggregation which runs a correlation function on
    the configured sibling multi-bucket aggregation.

    :arg function: (required) The correlation function to execute.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "bucket_correlation"

    def __init__(
        self,
        *,
        function: Union[
            "types.BucketCorrelationFunction", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(function=function, buckets_path=buckets_path, **kwargs)


class Cardinality(Agg[_R]):
    """
    A single-value metrics aggregation that calculates an approximate
    count of distinct values.

    :arg precision_threshold: A unique count below which counts are
        expected to be close to accurate. This allows to trade memory for
        accuracy. Defaults to `3000` if omitted.
    :arg rehash:
    :arg execution_hint: Mechanism by which cardinality aggregations is
        run.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "cardinality"

    def __init__(
        self,
        *,
        precision_threshold: Union[int, "DefaultType"] = DEFAULT,
        rehash: Union[bool, "DefaultType"] = DEFAULT,
        execution_hint: Union[
            Literal[
                "global_ordinals",
                "segment_ordinals",
                "direct",
                "save_memory_heuristic",
                "save_time_heuristic",
            ],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            precision_threshold=precision_threshold,
            rehash=rehash,
            execution_hint=execution_hint,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class CategorizeText(Bucket[_R]):
    """
    A multi-bucket aggregation that groups semi-structured text into
    buckets.

    :arg field: (required) The semi-structured text field to categorize.
    :arg max_unique_tokens: The maximum number of unique tokens at any
        position up to max_matched_tokens. Must be larger than 1. Smaller
        values use less memory and create fewer categories. Larger values
        will use more memory and create narrower categories. Max allowed
        value is 100. Defaults to `50` if omitted.
    :arg max_matched_tokens: The maximum number of token positions to
        match on before attempting to merge categories. Larger values will
        use more memory and create narrower categories. Max allowed value
        is 100. Defaults to `5` if omitted.
    :arg similarity_threshold: The minimum percentage of tokens that must
        match for text to be added to the category bucket. Must be between
        1 and 100. The larger the value the narrower the categories.
        Larger values will increase memory usage and create narrower
        categories. Defaults to `50` if omitted.
    :arg categorization_filters: This property expects an array of regular
        expressions. The expressions are used to filter out matching
        sequences from the categorization field values. You can use this
        functionality to fine tune the categorization by excluding
        sequences from consideration when categories are defined. For
        example, you can exclude SQL statements that appear in your log
        files. This property cannot be used at the same time as
        categorization_analyzer. If you only want to define simple regular
        expression filters that are applied prior to tokenization, setting
        this property is the easiest method. If you also want to customize
        the tokenizer or post-tokenization filtering, use the
        categorization_analyzer property instead and include the filters
        as pattern_replace character filters.
    :arg categorization_analyzer: The categorization analyzer specifies
        how the text is analyzed and tokenized before being categorized.
        The syntax is very similar to that used to define the analyzer in
        the analyze API. This property cannot be used at the same time as
        `categorization_filters`.
    :arg shard_size: The number of categorization buckets to return from
        each shard before merging all the results.
    :arg size: The number of buckets to return. Defaults to `10` if
        omitted.
    :arg min_doc_count: The minimum number of documents in a bucket to be
        returned to the results.
    :arg shard_min_doc_count: The minimum number of documents in a bucket
        to be returned from the shard before merging.
    """

    name = "categorize_text"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        max_unique_tokens: Union[int, "DefaultType"] = DEFAULT,
        max_matched_tokens: Union[int, "DefaultType"] = DEFAULT,
        similarity_threshold: Union[int, "DefaultType"] = DEFAULT,
        categorization_filters: Union[Sequence[str], "DefaultType"] = DEFAULT,
        categorization_analyzer: Union[
            str, "types.CustomCategorizeTextAnalyzer", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        shard_min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            max_unique_tokens=max_unique_tokens,
            max_matched_tokens=max_matched_tokens,
            similarity_threshold=similarity_threshold,
            categorization_filters=categorization_filters,
            categorization_analyzer=categorization_analyzer,
            shard_size=shard_size,
            size=size,
            min_doc_count=min_doc_count,
            shard_min_doc_count=shard_min_doc_count,
            **kwargs,
        )


class Children(Bucket[_R]):
    """
    A single bucket aggregation that selects child documents that have the
    specified type, as defined in a `join` field.

    :arg type: The child type that should be selected.
    """

    name = "children"

    def __init__(self, type: Union[str, "DefaultType"] = DEFAULT, **kwargs: Any):
        super().__init__(type=type, **kwargs)


class Composite(Bucket[_R]):
    """
    A multi-bucket aggregation that creates composite buckets from
    different sources. Unlike the other multi-bucket aggregations, you can
    use the `composite` aggregation to paginate *all* buckets from a
    multi-level aggregation efficiently.

    :arg after: When paginating, use the `after_key` value returned in the
        previous response to retrieve the next page.
    :arg size: The number of composite buckets that should be returned.
        Defaults to `10` if omitted.
    :arg sources: The value sources used to build composite buckets. Keys
        are returned in the order of the `sources` definition.
    """

    name = "composite"

    def __init__(
        self,
        *,
        after: Union[
            Mapping[
                Union[str, "InstrumentedField"], Union[int, float, str, bool, None]
            ],
            "DefaultType",
        ] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        sources: Union[Sequence[Mapping[str, Agg[_R]]], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(after=after, size=size, sources=sources, **kwargs)


class CumulativeCardinality(Pipeline[_R]):
    """
    A parent pipeline aggregation which calculates the cumulative
    cardinality in a parent `histogram` or `date_histogram` aggregation.

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "cumulative_cardinality"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class CumulativeSum(Pipeline[_R]):
    """
    A parent pipeline aggregation which calculates the cumulative sum of a
    specified metric in a parent `histogram` or `date_histogram`
    aggregation.

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "cumulative_sum"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class DateHistogram(Bucket[_R]):
    """
    A multi-bucket values source based aggregation that can be applied on
    date values or date range values extracted from the documents. It
    dynamically builds fixed size (interval) buckets over the values.

    :arg calendar_interval: Calendar-aware interval. Can be specified
        using the unit name, such as `month`, or as a single unit
        quantity, such as `1M`.
    :arg extended_bounds: Enables extending the bounds of the histogram
        beyond the data itself.
    :arg hard_bounds: Limits the histogram to specified bounds.
    :arg field: The date field whose values are use to build a histogram.
    :arg fixed_interval: Fixed intervals: a fixed number of SI units and
        never deviate, regardless of where they fall on the calendar.
    :arg format: The date format used to format `key_as_string` in the
        response. If no `format` is specified, the first date format
        specified in the field mapping is used.
    :arg interval:
    :arg min_doc_count: Only returns buckets that have `min_doc_count`
        number of documents. By default, all buckets between the first
        bucket that matches documents and the last one are returned.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg offset: Changes the start value of each bucket by the specified
        positive (`+`) or negative offset (`-`) duration.
    :arg order: The sort order of the returned buckets.
    :arg params:
    :arg script:
    :arg time_zone: Time zone used for bucketing and rounding. Defaults to
        Coordinated Universal Time (UTC).
    :arg keyed: Set to `true` to associate a unique string key with each
        bucket and return the ranges as a hash rather than an array.
    """

    name = "date_histogram"

    def __init__(
        self,
        *,
        calendar_interval: Union[
            Literal[
                "second", "minute", "hour", "day", "week", "month", "quarter", "year"
            ],
            "DefaultType",
        ] = DEFAULT,
        extended_bounds: Union[
            "types.ExtendedBounds", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        hard_bounds: Union[
            "types.ExtendedBounds", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        fixed_interval: Any = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        interval: Any = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        missing: Any = DEFAULT,
        offset: Any = DEFAULT,
        order: Union[
            Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]],
            Sequence[Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]]],
            "DefaultType",
        ] = DEFAULT,
        params: Union[Mapping[str, Any], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_zone: Union[str, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            calendar_interval=calendar_interval,
            extended_bounds=extended_bounds,
            hard_bounds=hard_bounds,
            field=field,
            fixed_interval=fixed_interval,
            format=format,
            interval=interval,
            min_doc_count=min_doc_count,
            missing=missing,
            offset=offset,
            order=order,
            params=params,
            script=script,
            time_zone=time_zone,
            keyed=keyed,
            **kwargs,
        )

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return FieldBucketData(self, search, data)


class DateRange(Bucket[_R]):
    """
    A multi-bucket value source based aggregation that enables the user to
    define a set of date ranges - each representing a bucket.

    :arg field: The date field whose values are use to build ranges.
    :arg format: The date format used to format `from` and `to` in the
        response.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg ranges: Array of date ranges.
    :arg time_zone: Time zone used to convert dates from another time zone
        to UTC.
    :arg keyed: Set to `true` to associate a unique string key with each
        bucket and returns the ranges as a hash rather than an array.
    """

    name = "date_range"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        ranges: Union[
            Sequence["wrappers.AggregationRange"],
            Sequence[Dict[str, Any]],
            "DefaultType",
        ] = DEFAULT,
        time_zone: Union[str, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            format=format,
            missing=missing,
            ranges=ranges,
            time_zone=time_zone,
            keyed=keyed,
            **kwargs,
        )


class Derivative(Pipeline[_R]):
    """
    A parent pipeline aggregation which calculates the derivative of a
    specified metric in a parent `histogram` or `date_histogram`
    aggregation.

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "derivative"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class DiversifiedSampler(Bucket[_R]):
    """
    A filtering aggregation used to limit any sub aggregations' processing
    to a sample of the top-scoring documents. Similar to the `sampler`
    aggregation, but adds the ability to limit the number of matches that
    share a common value.

    :arg execution_hint: The type of value used for de-duplication.
        Defaults to `global_ordinals` if omitted.
    :arg max_docs_per_value: Limits how many documents are permitted per
        choice of de-duplicating value. Defaults to `1` if omitted.
    :arg script:
    :arg shard_size: Limits how many top-scoring documents are collected
        in the sample processed on each shard. Defaults to `100` if
        omitted.
    :arg field: The field used to provide values used for de-duplication.
    """

    name = "diversified_sampler"

    def __init__(
        self,
        *,
        execution_hint: Union[
            Literal["map", "global_ordinals", "bytes_hash"], "DefaultType"
        ] = DEFAULT,
        max_docs_per_value: Union[int, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            execution_hint=execution_hint,
            max_docs_per_value=max_docs_per_value,
            script=script,
            shard_size=shard_size,
            field=field,
            **kwargs,
        )


class ExtendedStats(Agg[_R]):
    """
    A multi-value metrics aggregation that computes stats over numeric
    values extracted from the aggregated documents.

    :arg sigma: The number of standard deviations above/below the mean to
        display.
    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "extended_stats"

    def __init__(
        self,
        *,
        sigma: Union[float, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            sigma=sigma,
            format=format,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class ExtendedStatsBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which calculates a variety of stats
    across all bucket of a specified metric in a sibling aggregation.

    :arg sigma: The number of standard deviations above/below the mean to
        display.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "extended_stats_bucket"

    def __init__(
        self,
        *,
        sigma: Union[float, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            sigma=sigma,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class FrequentItemSets(Agg[_R]):
    """
    A bucket aggregation which finds frequent item sets, a form of
    association rules mining that identifies items that often occur
    together.

    :arg fields: (required) Fields to analyze.
    :arg minimum_set_size: The minimum size of one item set. Defaults to
        `1` if omitted.
    :arg minimum_support: The minimum support of one item set. Defaults to
        `0.1` if omitted.
    :arg size: The number of top item sets to return. Defaults to `10` if
        omitted.
    :arg filter: Query that filters documents from analysis.
    """

    name = "frequent_item_sets"
    _param_defs = {
        "filter": {"type": "query"},
    }

    def __init__(
        self,
        *,
        fields: Union[
            Sequence["types.FrequentItemSetsField"],
            Sequence[Dict[str, Any]],
            "DefaultType",
        ] = DEFAULT,
        minimum_set_size: Union[int, "DefaultType"] = DEFAULT,
        minimum_support: Union[float, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        filter: Union[Query, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            fields=fields,
            minimum_set_size=minimum_set_size,
            minimum_support=minimum_support,
            size=size,
            filter=filter,
            **kwargs,
        )


class Filter(Bucket[_R]):
    """
    A single bucket aggregation that narrows the set of documents to those
    that match a query.

    :arg filter: A single bucket aggregation that narrows the set of
        documents to those that match a query.
    """

    name = "filter"
    _param_defs = {
        "filter": {"type": "query"},
        "aggs": {"type": "agg", "hash": True},
    }

    def __init__(self, filter: Union[Query, "DefaultType"] = DEFAULT, **kwargs: Any):
        super().__init__(filter=filter, **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if isinstance(d[self.name], dict):
            n = cast(AttrDict[Any], d[self.name])
            n.update(n.pop("filter", {}))
        return d


class Filters(Bucket[_R]):
    """
    A multi-bucket aggregation where each bucket contains the documents
    that match a query.

    :arg filters: Collection of queries from which to build buckets.
    :arg other_bucket: Set to `true` to add a bucket to the response which
        will contain all documents that do not match any of the given
        filters.
    :arg other_bucket_key: The key with which the other bucket is
        returned. Defaults to `_other_` if omitted.
    :arg keyed: By default, the named filters aggregation returns the
        buckets as an object. Set to `false` to return the buckets as an
        array of objects. Defaults to `True` if omitted.
    """

    name = "filters"
    _param_defs = {
        "filters": {"type": "query", "hash": True},
        "aggs": {"type": "agg", "hash": True},
    }

    def __init__(
        self,
        *,
        filters: Union[Dict[str, Query], "DefaultType"] = DEFAULT,
        other_bucket: Union[bool, "DefaultType"] = DEFAULT,
        other_bucket_key: Union[str, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            filters=filters,
            other_bucket=other_bucket,
            other_bucket_key=other_bucket_key,
            keyed=keyed,
            **kwargs,
        )


class GeoBounds(Agg[_R]):
    """
    A metric aggregation that computes the geographic bounding box
    containing all values for a Geopoint or Geoshape field.

    :arg wrap_longitude: Specifies whether the bounding box should be
        allowed to overlap the international date line. Defaults to `True`
        if omitted.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "geo_bounds"

    def __init__(
        self,
        *,
        wrap_longitude: Union[bool, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            wrap_longitude=wrap_longitude,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class GeoCentroid(Agg[_R]):
    """
    A metric aggregation that computes the weighted centroid from all
    coordinate values for geo fields.

    :arg count:
    :arg location:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "geo_centroid"

    def __init__(
        self,
        *,
        count: Union[int, "DefaultType"] = DEFAULT,
        location: Union[
            "types.LatLonGeoLocation",
            "types.GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            count=count,
            location=location,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class GeoDistance(Bucket[_R]):
    """
    A multi-bucket aggregation that works on `geo_point` fields. Evaluates
    the distance of each document value from an origin point and
    determines the buckets it belongs to, based on ranges defined in the
    request.

    :arg distance_type: The distance calculation type. Defaults to `arc`
        if omitted.
    :arg field: A field of type `geo_point` used to evaluate the distance.
    :arg origin: The origin  used to evaluate the distance.
    :arg ranges: An array of ranges used to bucket documents.
    :arg unit: The distance unit. Defaults to `m` if omitted.
    """

    name = "geo_distance"

    def __init__(
        self,
        *,
        distance_type: Union[Literal["arc", "plane"], "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        origin: Union[
            "types.LatLonGeoLocation",
            "types.GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        ranges: Union[
            Sequence["wrappers.AggregationRange"],
            Sequence[Dict[str, Any]],
            "DefaultType",
        ] = DEFAULT,
        unit: Union[
            Literal["in", "ft", "yd", "mi", "nmi", "km", "m", "cm", "mm"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            distance_type=distance_type,
            field=field,
            origin=origin,
            ranges=ranges,
            unit=unit,
            **kwargs,
        )


class GeohashGrid(Bucket[_R]):
    """
    A multi-bucket aggregation that groups `geo_point` and `geo_shape`
    values into buckets that represent a grid. Each cell is labeled using
    a geohash which is of user-definable precision.

    :arg bounds: The bounding box to filter the points in each bucket.
    :arg field: Field containing indexed `geo_point` or `geo_shape`
        values. If the field contains an array, `geohash_grid` aggregates
        all array values.
    :arg precision: The string length of the geohashes used to define
        cells/buckets in the results. Defaults to `5` if omitted.
    :arg shard_size: Allows for more accurate counting of the top cells
        returned in the final result the aggregation. Defaults to
        returning `max(10,(size x number-of-shards))` buckets from each
        shard.
    :arg size: The maximum number of geohash buckets to return. Defaults
        to `10000` if omitted.
    """

    name = "geohash_grid"

    def __init__(
        self,
        *,
        bounds: Union[
            "types.CoordsGeoBounds",
            "types.TopLeftBottomRightGeoBounds",
            "types.TopRightBottomLeftGeoBounds",
            "types.WktGeoBounds",
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        precision: Union[float, str, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            bounds=bounds,
            field=field,
            precision=precision,
            shard_size=shard_size,
            size=size,
            **kwargs,
        )


class GeoLine(Agg[_R]):
    """
    Aggregates all `geo_point` values within a bucket into a `LineString`
    ordered by the chosen sort field.

    :arg point: (required) The name of the geo_point field.
    :arg sort: (required) The name of the numeric field to use as the sort
        key for ordering the points. When the `geo_line` aggregation is
        nested inside a `time_series` aggregation, this field defaults to
        `@timestamp`, and any other value will result in error.
    :arg include_sort: When `true`, returns an additional array of the
        sort values in the feature properties.
    :arg sort_order: The order in which the line is sorted (ascending or
        descending). Defaults to `asc` if omitted.
    :arg size: The maximum length of the line represented in the
        aggregation. Valid sizes are between 1 and 10000. Defaults to
        `10000` if omitted.
    """

    name = "geo_line"

    def __init__(
        self,
        *,
        point: Union["types.GeoLinePoint", Dict[str, Any], "DefaultType"] = DEFAULT,
        sort: Union["types.GeoLineSort", Dict[str, Any], "DefaultType"] = DEFAULT,
        include_sort: Union[bool, "DefaultType"] = DEFAULT,
        sort_order: Union[Literal["asc", "desc"], "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            point=point,
            sort=sort,
            include_sort=include_sort,
            sort_order=sort_order,
            size=size,
            **kwargs,
        )


class GeotileGrid(Bucket[_R]):
    """
    A multi-bucket aggregation that groups `geo_point` and `geo_shape`
    values into buckets that represent a grid. Each cell corresponds to a
    map tile as used by many online map sites.

    :arg field: Field containing indexed `geo_point` or `geo_shape`
        values. If the field contains an array, `geotile_grid` aggregates
        all array values.
    :arg precision: Integer zoom of the key used to define cells/buckets
        in the results. Values outside of the range [0,29] will be
        rejected. Defaults to `7` if omitted.
    :arg shard_size: Allows for more accurate counting of the top cells
        returned in the final result the aggregation. Defaults to
        returning `max(10,(size x number-of-shards))` buckets from each
        shard.
    :arg size: The maximum number of buckets to return. Defaults to
        `10000` if omitted.
    :arg bounds: A bounding box to filter the geo-points or geo-shapes in
        each bucket.
    """

    name = "geotile_grid"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        precision: Union[float, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        bounds: Union[
            "types.CoordsGeoBounds",
            "types.TopLeftBottomRightGeoBounds",
            "types.TopRightBottomLeftGeoBounds",
            "types.WktGeoBounds",
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            precision=precision,
            shard_size=shard_size,
            size=size,
            bounds=bounds,
            **kwargs,
        )


class GeohexGrid(Bucket[_R]):
    """
    A multi-bucket aggregation that groups `geo_point` and `geo_shape`
    values into buckets that represent a grid. Each cell corresponds to a
    H3 cell index and is labeled using the H3Index representation.

    :arg field: (required) Field containing indexed `geo_point` or
        `geo_shape` values. If the field contains an array, `geohex_grid`
        aggregates all array values.
    :arg precision: Integer zoom of the key used to defined cells or
        buckets in the results. Value should be between 0-15. Defaults to
        `6` if omitted.
    :arg bounds: Bounding box used to filter the geo-points in each
        bucket.
    :arg size: Maximum number of buckets to return. Defaults to `10000` if
        omitted.
    :arg shard_size: Number of buckets returned from each shard.
    """

    name = "geohex_grid"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        precision: Union[int, "DefaultType"] = DEFAULT,
        bounds: Union[
            "types.CoordsGeoBounds",
            "types.TopLeftBottomRightGeoBounds",
            "types.TopRightBottomLeftGeoBounds",
            "types.WktGeoBounds",
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            precision=precision,
            bounds=bounds,
            size=size,
            shard_size=shard_size,
            **kwargs,
        )


class Global(Bucket[_R]):
    """
    Defines a single bucket of all the documents within the search
    execution context. This context is defined by the indices and the
    document types you’re searching on, but is not influenced by the
    search query itself.
    """

    name = "global"

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)


class Histogram(Bucket[_R]):
    """
    A multi-bucket values source based aggregation that can be applied on
    numeric values or numeric range values extracted from the documents.
    It dynamically builds fixed size (interval) buckets over the values.

    :arg extended_bounds: Enables extending the bounds of the histogram
        beyond the data itself.
    :arg hard_bounds: Limits the range of buckets in the histogram. It is
        particularly useful in the case of open data ranges that can
        result in a very large number of buckets.
    :arg field: The name of the field to aggregate on.
    :arg interval: The interval for the buckets. Must be a positive
        decimal.
    :arg min_doc_count: Only returns buckets that have `min_doc_count`
        number of documents. By default, the response will fill gaps in
        the histogram with empty buckets.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg offset: By default, the bucket keys start with 0 and then
        continue in even spaced steps of `interval`. The bucket boundaries
        can be shifted by using the `offset` option.
    :arg order: The sort order of the returned buckets. By default, the
        returned buckets are sorted by their key ascending.
    :arg script:
    :arg format:
    :arg keyed: If `true`, returns buckets as a hash instead of an array,
        keyed by the bucket keys.
    """

    name = "histogram"

    def __init__(
        self,
        *,
        extended_bounds: Union[
            "types.ExtendedBounds", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        hard_bounds: Union[
            "types.ExtendedBounds", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        interval: Union[float, "DefaultType"] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        missing: Union[float, "DefaultType"] = DEFAULT,
        offset: Union[float, "DefaultType"] = DEFAULT,
        order: Union[
            Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]],
            Sequence[Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]]],
            "DefaultType",
        ] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            extended_bounds=extended_bounds,
            hard_bounds=hard_bounds,
            field=field,
            interval=interval,
            min_doc_count=min_doc_count,
            missing=missing,
            offset=offset,
            order=order,
            script=script,
            format=format,
            keyed=keyed,
            **kwargs,
        )

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return FieldBucketData(self, search, data)


class IPRange(Bucket[_R]):
    """
    A multi-bucket value source based aggregation that enables the user to
    define a set of IP ranges - each representing a bucket.

    :arg field: The date field whose values are used to build ranges.
    :arg ranges: Array of IP ranges.
    """

    name = "ip_range"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        ranges: Union[
            Sequence["types.IpRangeAggregationRange"],
            Sequence[Dict[str, Any]],
            "DefaultType",
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(field=field, ranges=ranges, **kwargs)


class IPPrefix(Bucket[_R]):
    """
    A bucket aggregation that groups documents based on the network or
    sub-network of an IP address.

    :arg field: (required) The IP address field to aggregation on. The
        field mapping type must be `ip`.
    :arg prefix_length: (required) Length of the network prefix. For IPv4
        addresses the accepted range is [0, 32]. For IPv6 addresses the
        accepted range is [0, 128].
    :arg is_ipv6: Defines whether the prefix applies to IPv6 addresses.
    :arg append_prefix_length: Defines whether the prefix length is
        appended to IP address keys in the response.
    :arg keyed: Defines whether buckets are returned as a hash rather than
        an array in the response.
    :arg min_doc_count: Minimum number of documents in a bucket for it to
        be included in the response. Defaults to `1` if omitted.
    """

    name = "ip_prefix"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        prefix_length: Union[int, "DefaultType"] = DEFAULT,
        is_ipv6: Union[bool, "DefaultType"] = DEFAULT,
        append_prefix_length: Union[bool, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            prefix_length=prefix_length,
            is_ipv6=is_ipv6,
            append_prefix_length=append_prefix_length,
            keyed=keyed,
            min_doc_count=min_doc_count,
            **kwargs,
        )


class Inference(Pipeline[_R]):
    """
    A parent pipeline aggregation which loads a pre-trained model and
    performs inference on the collated result fields from the parent
    bucket aggregation.

    :arg model_id: (required) The ID or alias for the trained model.
    :arg inference_config: Contains the inference type and its options.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "inference"

    def __init__(
        self,
        *,
        model_id: Union[str, "DefaultType"] = DEFAULT,
        inference_config: Union[
            "types.InferenceConfigContainer", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            model_id=model_id,
            inference_config=inference_config,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class Line(Agg[_R]):
    """
    :arg point: (required) The name of the geo_point field.
    :arg sort: (required) The name of the numeric field to use as the sort
        key for ordering the points. When the `geo_line` aggregation is
        nested inside a `time_series` aggregation, this field defaults to
        `@timestamp`, and any other value will result in error.
    :arg include_sort: When `true`, returns an additional array of the
        sort values in the feature properties.
    :arg sort_order: The order in which the line is sorted (ascending or
        descending). Defaults to `asc` if omitted.
    :arg size: The maximum length of the line represented in the
        aggregation. Valid sizes are between 1 and 10000. Defaults to
        `10000` if omitted.
    """

    name = "line"

    def __init__(
        self,
        *,
        point: Union["types.GeoLinePoint", Dict[str, Any], "DefaultType"] = DEFAULT,
        sort: Union["types.GeoLineSort", Dict[str, Any], "DefaultType"] = DEFAULT,
        include_sort: Union[bool, "DefaultType"] = DEFAULT,
        sort_order: Union[Literal["asc", "desc"], "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            point=point,
            sort=sort,
            include_sort=include_sort,
            sort_order=sort_order,
            size=size,
            **kwargs,
        )


class MatrixStats(Agg[_R]):
    """
    A numeric aggregation that computes the following statistics over a
    set of document fields: `count`, `mean`, `variance`, `skewness`,
    `kurtosis`, `covariance`, and `covariance`.

    :arg mode: Array value the aggregation will use for array or multi-
        valued fields. Defaults to `avg` if omitted.
    :arg fields: An array of fields for computing the statistics.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    """

    name = "matrix_stats"

    def __init__(
        self,
        *,
        mode: Union[
            Literal["min", "max", "sum", "avg", "median"], "DefaultType"
        ] = DEFAULT,
        fields: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        missing: Union[
            Mapping[Union[str, "InstrumentedField"], float], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(mode=mode, fields=fields, missing=missing, **kwargs)


class Max(Agg[_R]):
    """
    A single-value metrics aggregation that returns the maximum value
    among the numeric values extracted from the aggregated documents.

    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "max"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, field=field, missing=missing, script=script, **kwargs
        )


class MaxBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which identifies the bucket(s) with the
    maximum value of a specified metric in a sibling aggregation and
    outputs both the value and the key(s) of the bucket(s).

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "max_bucket"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class MedianAbsoluteDeviation(Agg[_R]):
    """
    A single-value aggregation that approximates the median absolute
    deviation of its search results.

    :arg compression: Limits the maximum number of nodes used by the
        underlying TDigest algorithm to `20 * compression`, enabling
        control of memory usage and approximation error. Defaults to
        `1000` if omitted.
    :arg execution_hint: The default implementation of TDigest is
        optimized for performance, scaling to millions or even billions of
        sample values while maintaining acceptable accuracy levels (close
        to 1% relative error for millions of samples in some cases). To
        use an implementation optimized for accuracy, set this parameter
        to high_accuracy instead. Defaults to `default` if omitted.
    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "median_absolute_deviation"

    def __init__(
        self,
        *,
        compression: Union[float, "DefaultType"] = DEFAULT,
        execution_hint: Union[
            Literal["default", "high_accuracy"], "DefaultType"
        ] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            compression=compression,
            execution_hint=execution_hint,
            format=format,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class Min(Agg[_R]):
    """
    A single-value metrics aggregation that returns the minimum value
    among numeric values extracted from the aggregated documents.

    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "min"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, field=field, missing=missing, script=script, **kwargs
        )


class MinBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which identifies the bucket(s) with the
    minimum value of a specified metric in a sibling aggregation and
    outputs both the value and the key(s) of the bucket(s).

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "min_bucket"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class Missing(Bucket[_R]):
    """
    A field data based single bucket aggregation, that creates a bucket of
    all documents in the current document set context that are missing a
    field value (effectively, missing a field or having the configured
    NULL value set).

    :arg field: The name of the field.
    :arg missing:
    """

    name = "missing"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(field=field, missing=missing, **kwargs)


class MovingAvg(Pipeline[_R]):
    """ """

    name = "moving_avg"

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)


class LinearMovingAverageAggregation(MovingAvg[_R]):
    """
    :arg model: (required)
    :arg settings: (required)
    :arg minimize:
    :arg predict:
    :arg window:
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    def __init__(
        self,
        *,
        model: Any = DEFAULT,
        settings: Union["types.EmptyObject", Dict[str, Any], "DefaultType"] = DEFAULT,
        minimize: Union[bool, "DefaultType"] = DEFAULT,
        predict: Union[int, "DefaultType"] = DEFAULT,
        window: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            model=model,
            settings=settings,
            minimize=minimize,
            predict=predict,
            window=window,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class SimpleMovingAverageAggregation(MovingAvg[_R]):
    """
    :arg model: (required)
    :arg settings: (required)
    :arg minimize:
    :arg predict:
    :arg window:
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    def __init__(
        self,
        *,
        model: Any = DEFAULT,
        settings: Union["types.EmptyObject", Dict[str, Any], "DefaultType"] = DEFAULT,
        minimize: Union[bool, "DefaultType"] = DEFAULT,
        predict: Union[int, "DefaultType"] = DEFAULT,
        window: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            model=model,
            settings=settings,
            minimize=minimize,
            predict=predict,
            window=window,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class EwmaMovingAverageAggregation(MovingAvg[_R]):
    """
    :arg model: (required)
    :arg settings: (required)
    :arg minimize:
    :arg predict:
    :arg window:
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    def __init__(
        self,
        *,
        model: Any = DEFAULT,
        settings: Union[
            "types.EwmaModelSettings", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        minimize: Union[bool, "DefaultType"] = DEFAULT,
        predict: Union[int, "DefaultType"] = DEFAULT,
        window: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            model=model,
            settings=settings,
            minimize=minimize,
            predict=predict,
            window=window,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class HoltMovingAverageAggregation(MovingAvg[_R]):
    """
    :arg model: (required)
    :arg settings: (required)
    :arg minimize:
    :arg predict:
    :arg window:
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    def __init__(
        self,
        *,
        model: Any = DEFAULT,
        settings: Union[
            "types.HoltLinearModelSettings", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        minimize: Union[bool, "DefaultType"] = DEFAULT,
        predict: Union[int, "DefaultType"] = DEFAULT,
        window: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            model=model,
            settings=settings,
            minimize=minimize,
            predict=predict,
            window=window,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class HoltWintersMovingAverageAggregation(MovingAvg[_R]):
    """
    :arg model: (required)
    :arg settings: (required)
    :arg minimize:
    :arg predict:
    :arg window:
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    def __init__(
        self,
        *,
        model: Any = DEFAULT,
        settings: Union[
            "types.HoltWintersModelSettings", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        minimize: Union[bool, "DefaultType"] = DEFAULT,
        predict: Union[int, "DefaultType"] = DEFAULT,
        window: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            model=model,
            settings=settings,
            minimize=minimize,
            predict=predict,
            window=window,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class MovingPercentiles(Pipeline[_R]):
    """
    Given an ordered series of percentiles, "slides" a window across those
    percentiles and computes cumulative percentiles.

    :arg window: The size of window to "slide" across the histogram.
    :arg shift: By default, the window consists of the last n values
        excluding the current bucket. Increasing `shift` by 1, moves the
        starting window position by 1 to the right.
    :arg keyed:
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "moving_percentiles"

    def __init__(
        self,
        *,
        window: Union[int, "DefaultType"] = DEFAULT,
        shift: Union[int, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            window=window,
            shift=shift,
            keyed=keyed,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class MovingFn(Pipeline[_R]):
    """
    Given an ordered series of data, "slides" a window across the data and
    runs a custom script on each window of data. For convenience, a number
    of common functions are predefined such as `min`, `max`, and moving
    averages.

    :arg script: The script that should be executed on each window of
        data.
    :arg shift: By default, the window consists of the last n values
        excluding the current bucket. Increasing `shift` by 1, moves the
        starting window position by 1 to the right.
    :arg window: The size of window to "slide" across the histogram.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "moving_fn"

    def __init__(
        self,
        *,
        script: Union[str, "DefaultType"] = DEFAULT,
        shift: Union[int, "DefaultType"] = DEFAULT,
        window: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            script=script,
            shift=shift,
            window=window,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class MultiTerms(Bucket[_R]):
    """
    A multi-bucket value source based aggregation where buckets are
    dynamically built - one per unique set of values.

    :arg terms: (required) The field from which to generate sets of terms.
    :arg collect_mode: Specifies the strategy for data collection.
        Defaults to `breadth_first` if omitted.
    :arg order: Specifies the sort order of the buckets. Defaults to
        sorting by descending document count.
    :arg min_doc_count: The minimum number of documents in a bucket for it
        to be returned. Defaults to `1` if omitted.
    :arg shard_min_doc_count: The minimum number of documents in a bucket
        on each shard for it to be returned. Defaults to `1` if omitted.
    :arg shard_size: The number of candidate terms produced by each shard.
        By default, `shard_size` will be automatically estimated based on
        the number of shards and the `size` parameter.
    :arg show_term_doc_count_error: Calculates the doc count error on per
        term basis.
    :arg size: The number of term buckets should be returned out of the
        overall terms list. Defaults to `10` if omitted.
    """

    name = "multi_terms"

    def __init__(
        self,
        *,
        terms: Union[
            Sequence["types.MultiTermLookup"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        collect_mode: Union[
            Literal["depth_first", "breadth_first"], "DefaultType"
        ] = DEFAULT,
        order: Union[
            Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]],
            Sequence[Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]]],
            "DefaultType",
        ] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        shard_min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        show_term_doc_count_error: Union[bool, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            terms=terms,
            collect_mode=collect_mode,
            order=order,
            min_doc_count=min_doc_count,
            shard_min_doc_count=shard_min_doc_count,
            shard_size=shard_size,
            show_term_doc_count_error=show_term_doc_count_error,
            size=size,
            **kwargs,
        )


class Nested(Bucket[_R]):
    """
    A special single bucket aggregation that enables aggregating nested
    documents.

    :arg path: The path to the field of type `nested`.
    """

    name = "nested"

    def __init__(
        self,
        path: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(path=path, **kwargs)


class Normalize(Pipeline[_R]):
    """
    A parent pipeline aggregation which calculates the specific
    normalized/rescaled value for a specific bucket value.

    :arg method: The specific method to apply.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "normalize"

    def __init__(
        self,
        *,
        method: Union[
            Literal[
                "rescale_0_1",
                "rescale_0_100",
                "percent_of_sum",
                "mean",
                "z-score",
                "softmax",
            ],
            "DefaultType",
        ] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            method=method,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class Parent(Bucket[_R]):
    """
    A special single bucket aggregation that selects parent documents that
    have the specified type, as defined in a `join` field.

    :arg type: The child type that should be selected.
    """

    name = "parent"

    def __init__(self, type: Union[str, "DefaultType"] = DEFAULT, **kwargs: Any):
        super().__init__(type=type, **kwargs)


class PercentileRanks(Agg[_R]):
    """
    A multi-value metrics aggregation that calculates one or more
    percentile ranks over numeric values extracted from the aggregated
    documents.

    :arg keyed: By default, the aggregation associates a unique string key
        with each bucket and returns the ranges as a hash rather than an
        array. Set to `false` to disable this behavior. Defaults to `True`
        if omitted.
    :arg values: An array of values for which to calculate the percentile
        ranks.
    :arg hdr: Uses the alternative High Dynamic Range Histogram algorithm
        to calculate percentile ranks.
    :arg tdigest: Sets parameters for the default TDigest algorithm used
        to calculate percentile ranks.
    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "percentile_ranks"

    def __init__(
        self,
        *,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        values: Union[Sequence[float], None, "DefaultType"] = DEFAULT,
        hdr: Union["types.HdrMethod", Dict[str, Any], "DefaultType"] = DEFAULT,
        tdigest: Union["types.TDigest", Dict[str, Any], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            keyed=keyed,
            values=values,
            hdr=hdr,
            tdigest=tdigest,
            format=format,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class Percentiles(Agg[_R]):
    """
    A multi-value metrics aggregation that calculates one or more
    percentiles over numeric values extracted from the aggregated
    documents.

    :arg keyed: By default, the aggregation associates a unique string key
        with each bucket and returns the ranges as a hash rather than an
        array. Set to `false` to disable this behavior. Defaults to `True`
        if omitted.
    :arg percents: The percentiles to calculate.
    :arg hdr: Uses the alternative High Dynamic Range Histogram algorithm
        to calculate percentiles.
    :arg tdigest: Sets parameters for the default TDigest algorithm used
        to calculate percentiles.
    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "percentiles"

    def __init__(
        self,
        *,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        percents: Union[Sequence[float], "DefaultType"] = DEFAULT,
        hdr: Union["types.HdrMethod", Dict[str, Any], "DefaultType"] = DEFAULT,
        tdigest: Union["types.TDigest", Dict[str, Any], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            keyed=keyed,
            percents=percents,
            hdr=hdr,
            tdigest=tdigest,
            format=format,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class PercentilesBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which calculates percentiles across all
    bucket of a specified metric in a sibling aggregation.

    :arg percents: The list of percentiles to calculate.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "percentiles_bucket"

    def __init__(
        self,
        *,
        percents: Union[Sequence[float], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            percents=percents,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class Range(Bucket[_R]):
    """
    A multi-bucket value source based aggregation that enables the user to
    define a set of ranges - each representing a bucket.

    :arg field: The date field whose values are use to build ranges.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg ranges: An array of ranges used to bucket documents.
    :arg script:
    :arg keyed: Set to `true` to associate a unique string key with each
        bucket and return the ranges as a hash rather than an array.
    :arg format:
    """

    name = "range"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[int, "DefaultType"] = DEFAULT,
        ranges: Union[
            Sequence["wrappers.AggregationRange"],
            Sequence[Dict[str, Any]],
            "DefaultType",
        ] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            missing=missing,
            ranges=ranges,
            script=script,
            keyed=keyed,
            format=format,
            **kwargs,
        )


class RareTerms(Bucket[_R]):
    """
    A multi-bucket value source based aggregation which finds "rare"
    terms — terms that are at the long-tail of the distribution and are
    not frequent.

    :arg exclude: Terms that should be excluded from the aggregation.
    :arg field: The field from which to return rare terms.
    :arg include: Terms that should be included in the aggregation.
    :arg max_doc_count: The maximum number of documents a term should
        appear in. Defaults to `1` if omitted.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg precision: The precision of the internal CuckooFilters. Smaller
        precision leads to better approximation, but higher memory usage.
        Defaults to `0.001` if omitted.
    :arg value_type:
    """

    name = "rare_terms"

    def __init__(
        self,
        *,
        exclude: Union[str, Sequence[str], "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        include: Union[
            str, Sequence[str], "types.TermsPartition", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        max_doc_count: Union[int, "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        precision: Union[float, "DefaultType"] = DEFAULT,
        value_type: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            exclude=exclude,
            field=field,
            include=include,
            max_doc_count=max_doc_count,
            missing=missing,
            precision=precision,
            value_type=value_type,
            **kwargs,
        )


class Rate(Agg[_R]):
    """
    Calculates a rate of documents or a field in each bucket. Can only be
    used inside a `date_histogram` or `composite` aggregation.

    :arg unit: The interval used to calculate the rate. By default, the
        interval of the `date_histogram` is used.
    :arg mode: How the rate is calculated. Defaults to `sum` if omitted.
    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "rate"

    def __init__(
        self,
        *,
        unit: Union[
            Literal[
                "second", "minute", "hour", "day", "week", "month", "quarter", "year"
            ],
            "DefaultType",
        ] = DEFAULT,
        mode: Union[Literal["sum", "value_count"], "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            unit=unit,
            mode=mode,
            format=format,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class ReverseNested(Bucket[_R]):
    """
    A special single bucket aggregation that enables aggregating on parent
    documents from nested documents. Should only be defined inside a
    `nested` aggregation.

    :arg path: Defines the nested object field that should be joined back
        to. The default is empty, which means that it joins back to the
        root/main document level.
    """

    name = "reverse_nested"

    def __init__(
        self,
        path: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(path=path, **kwargs)


class RandomSampler(Bucket[_R]):
    """
    A single bucket aggregation that randomly includes documents in the
    aggregated results. Sampling provides significant speed improvement at
    the cost of accuracy.

    :arg probability: (required) The probability that a document will be
        included in the aggregated data. Must be greater than 0, less than
        0.5, or exactly 1. The lower the probability, the fewer documents
        are matched.
    :arg seed: The seed to generate the random sampling of documents. When
        a seed is provided, the random subset of documents is the same
        between calls.
    :arg shard_seed: When combined with seed, setting shard_seed ensures
        100% consistent sampling over shards where data is exactly the
        same.
    """

    name = "random_sampler"

    def __init__(
        self,
        *,
        probability: Union[float, "DefaultType"] = DEFAULT,
        seed: Union[int, "DefaultType"] = DEFAULT,
        shard_seed: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            probability=probability, seed=seed, shard_seed=shard_seed, **kwargs
        )


class Sampler(Bucket[_R]):
    """
    A filtering aggregation used to limit any sub aggregations' processing
    to a sample of the top-scoring documents.

    :arg shard_size: Limits how many top-scoring documents are collected
        in the sample processed on each shard. Defaults to `100` if
        omitted.
    """

    name = "sampler"

    def __init__(self, shard_size: Union[int, "DefaultType"] = DEFAULT, **kwargs: Any):
        super().__init__(shard_size=shard_size, **kwargs)


class ScriptedMetric(Agg[_R]):
    """
    A metric aggregation that uses scripts to provide a metric output.

    :arg combine_script: Runs once on each shard after document collection
        is complete. Allows the aggregation to consolidate the state
        returned from each shard.
    :arg init_script: Runs prior to any collection of documents. Allows
        the aggregation to set up any initial state.
    :arg map_script: Run once per document collected. If no
        `combine_script` is specified, the resulting state needs to be
        stored in the `state` object.
    :arg params: A global object with script parameters for `init`, `map`
        and `combine` scripts. It is shared between the scripts.
    :arg reduce_script: Runs once on the coordinating node after all
        shards have returned their results. The script is provided with
        access to a variable `states`, which is an array of the result of
        the `combine_script` on each shard.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "scripted_metric"

    def __init__(
        self,
        *,
        combine_script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        init_script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        map_script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        params: Union[Mapping[str, Any], "DefaultType"] = DEFAULT,
        reduce_script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            combine_script=combine_script,
            init_script=init_script,
            map_script=map_script,
            params=params,
            reduce_script=reduce_script,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class SerialDiff(Pipeline[_R]):
    """
    An aggregation that subtracts values in a time series from themselves
    at different time lags or periods.

    :arg lag: The historical bucket to subtract from the current value.
        Must be a positive, non-zero integer.
    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "serial_diff"

    def __init__(
        self,
        *,
        lag: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            lag=lag,
            format=format,
            gap_policy=gap_policy,
            buckets_path=buckets_path,
            **kwargs,
        )


class SignificantTerms(Bucket[_R]):
    """
    Returns interesting or unusual occurrences of terms in a set.

    :arg background_filter: A background filter that can be used to focus
        in on significant terms within a narrower context, instead of the
        entire index.
    :arg chi_square: Use Chi square, as described in "Information
        Retrieval", Manning et al., Chapter 13.5.2, as the significance
        score.
    :arg exclude: Terms to exclude.
    :arg execution_hint: Mechanism by which the aggregation should be
        executed: using field values directly or using global ordinals.
    :arg field: The field from which to return significant terms.
    :arg gnd: Use Google normalized distance as described in "The Google
        Similarity Distance", Cilibrasi and Vitanyi, 2007, as the
        significance score.
    :arg include: Terms to include.
    :arg jlh: Use JLH score as the significance score.
    :arg min_doc_count: Only return terms that are found in more than
        `min_doc_count` hits. Defaults to `3` if omitted.
    :arg mutual_information: Use mutual information as described in
        "Information Retrieval", Manning et al., Chapter 13.5.1, as the
        significance score.
    :arg percentage: A simple calculation of the number of documents in
        the foreground sample with a term divided by the number of
        documents in the background with the term.
    :arg script_heuristic: Customized score, implemented via a script.
    :arg shard_min_doc_count: Regulates the certainty a shard has if the
        term should actually be added to the candidate list or not with
        respect to the `min_doc_count`. Terms will only be considered if
        their local shard frequency within the set is higher than the
        `shard_min_doc_count`.
    :arg shard_size: Can be used to control the volumes of candidate terms
        produced by each shard. By default, `shard_size` will be
        automatically estimated based on the number of shards and the
        `size` parameter.
    :arg size: The number of buckets returned out of the overall terms
        list.
    """

    name = "significant_terms"
    _param_defs = {
        "background_filter": {"type": "query"},
    }

    def __init__(
        self,
        *,
        background_filter: Union[Query, "DefaultType"] = DEFAULT,
        chi_square: Union[
            "types.ChiSquareHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        exclude: Union[str, Sequence[str], "DefaultType"] = DEFAULT,
        execution_hint: Union[
            Literal[
                "map",
                "global_ordinals",
                "global_ordinals_hash",
                "global_ordinals_low_cardinality",
            ],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        gnd: Union[
            "types.GoogleNormalizedDistanceHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        include: Union[
            str, Sequence[str], "types.TermsPartition", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        jlh: Union["types.EmptyObject", Dict[str, Any], "DefaultType"] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        mutual_information: Union[
            "types.MutualInformationHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        percentage: Union[
            "types.PercentageScoreHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        script_heuristic: Union[
            "types.ScriptedHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        shard_min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            background_filter=background_filter,
            chi_square=chi_square,
            exclude=exclude,
            execution_hint=execution_hint,
            field=field,
            gnd=gnd,
            include=include,
            jlh=jlh,
            min_doc_count=min_doc_count,
            mutual_information=mutual_information,
            percentage=percentage,
            script_heuristic=script_heuristic,
            shard_min_doc_count=shard_min_doc_count,
            shard_size=shard_size,
            size=size,
            **kwargs,
        )


class SignificantText(Bucket[_R]):
    """
    Returns interesting or unusual occurrences of free-text terms in a
    set.

    :arg background_filter: A background filter that can be used to focus
        in on significant terms within a narrower context, instead of the
        entire index.
    :arg chi_square: Use Chi square, as described in "Information
        Retrieval", Manning et al., Chapter 13.5.2, as the significance
        score.
    :arg exclude: Values to exclude.
    :arg execution_hint: Determines whether the aggregation will use field
        values directly or global ordinals.
    :arg field: The field from which to return significant text.
    :arg filter_duplicate_text: Whether to out duplicate text to deal with
        noisy data.
    :arg gnd: Use Google normalized distance as described in "The Google
        Similarity Distance", Cilibrasi and Vitanyi, 2007, as the
        significance score.
    :arg include: Values to include.
    :arg jlh: Use JLH score as the significance score.
    :arg min_doc_count: Only return values that are found in more than
        `min_doc_count` hits. Defaults to `3` if omitted.
    :arg mutual_information: Use mutual information as described in
        "Information Retrieval", Manning et al., Chapter 13.5.1, as the
        significance score.
    :arg percentage: A simple calculation of the number of documents in
        the foreground sample with a term divided by the number of
        documents in the background with the term.
    :arg script_heuristic: Customized score, implemented via a script.
    :arg shard_min_doc_count: Regulates the certainty a shard has if the
        values should actually be added to the candidate list or not with
        respect to the min_doc_count. Values will only be considered if
        their local shard frequency within the set is higher than the
        `shard_min_doc_count`.
    :arg shard_size: The number of candidate terms produced by each shard.
        By default, `shard_size` will be automatically estimated based on
        the number of shards and the `size` parameter.
    :arg size: The number of buckets returned out of the overall terms
        list.
    :arg source_fields: Overrides the JSON `_source` fields from which
        text will be analyzed.
    """

    name = "significant_text"
    _param_defs = {
        "background_filter": {"type": "query"},
    }

    def __init__(
        self,
        *,
        background_filter: Union[Query, "DefaultType"] = DEFAULT,
        chi_square: Union[
            "types.ChiSquareHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        exclude: Union[str, Sequence[str], "DefaultType"] = DEFAULT,
        execution_hint: Union[
            Literal[
                "map",
                "global_ordinals",
                "global_ordinals_hash",
                "global_ordinals_low_cardinality",
            ],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        filter_duplicate_text: Union[bool, "DefaultType"] = DEFAULT,
        gnd: Union[
            "types.GoogleNormalizedDistanceHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        include: Union[
            str, Sequence[str], "types.TermsPartition", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        jlh: Union["types.EmptyObject", Dict[str, Any], "DefaultType"] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        mutual_information: Union[
            "types.MutualInformationHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        percentage: Union[
            "types.PercentageScoreHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        script_heuristic: Union[
            "types.ScriptedHeuristic", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        shard_min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        source_fields: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            background_filter=background_filter,
            chi_square=chi_square,
            exclude=exclude,
            execution_hint=execution_hint,
            field=field,
            filter_duplicate_text=filter_duplicate_text,
            gnd=gnd,
            include=include,
            jlh=jlh,
            min_doc_count=min_doc_count,
            mutual_information=mutual_information,
            percentage=percentage,
            script_heuristic=script_heuristic,
            shard_min_doc_count=shard_min_doc_count,
            shard_size=shard_size,
            size=size,
            source_fields=source_fields,
            **kwargs,
        )


class Stats(Agg[_R]):
    """
    A multi-value metrics aggregation that computes stats over numeric
    values extracted from the aggregated documents.

    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "stats"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, field=field, missing=missing, script=script, **kwargs
        )


class StatsBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which calculates a variety of stats
    across all bucket of a specified metric in a sibling aggregation.

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "stats_bucket"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class StringStats(Agg[_R]):
    """
    A multi-value metrics aggregation that computes statistics over string
    values extracted from the aggregated documents.

    :arg show_distribution: Shows the probability distribution for all
        characters.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "string_stats"

    def __init__(
        self,
        *,
        show_distribution: Union[bool, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            show_distribution=show_distribution,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class Sum(Agg[_R]):
    """
    A single-value metrics aggregation that sums numeric values that are
    extracted from the aggregated documents.

    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "sum"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, field=field, missing=missing, script=script, **kwargs
        )


class SumBucket(Pipeline[_R]):
    """
    A sibling pipeline aggregation which calculates the sum of a specified
    metric across all buckets in a sibling aggregation.

    :arg format: `DecimalFormat` pattern for the output value. If
        specified, the formatted value is returned in the aggregation’s
        `value_as_string` property.
    :arg gap_policy: Policy to apply when gaps are found in the data.
        Defaults to `skip` if omitted.
    :arg buckets_path: Path to the buckets that contain one set of values
        to correlate.
    """

    name = "sum_bucket"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        gap_policy: Union[
            Literal["skip", "insert_zeros", "keep_values"], "DefaultType"
        ] = DEFAULT,
        buckets_path: Union[
            str, Sequence[str], Mapping[str, str], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, gap_policy=gap_policy, buckets_path=buckets_path, **kwargs
        )


class Terms(Bucket[_R]):
    """
    A multi-bucket value source based aggregation where buckets are
    dynamically built - one per unique value.

    :arg collect_mode: Determines how child aggregations should be
        calculated: breadth-first or depth-first.
    :arg exclude: Values to exclude. Accepts regular expressions and
        partitions.
    :arg execution_hint: Determines whether the aggregation will use field
        values directly or global ordinals.
    :arg field: The field from which to return terms.
    :arg include: Values to include. Accepts regular expressions and
        partitions.
    :arg min_doc_count: Only return values that are found in more than
        `min_doc_count` hits. Defaults to `1` if omitted.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg missing_order:
    :arg missing_bucket:
    :arg value_type: Coerced unmapped fields into the specified type.
    :arg order: Specifies the sort order of the buckets. Defaults to
        sorting by descending document count.
    :arg script:
    :arg shard_min_doc_count: Regulates the certainty a shard has if the
        term should actually be added to the candidate list or not with
        respect to the `min_doc_count`. Terms will only be considered if
        their local shard frequency within the set is higher than the
        `shard_min_doc_count`.
    :arg shard_size: The number of candidate terms produced by each shard.
        By default, `shard_size` will be automatically estimated based on
        the number of shards and the `size` parameter.
    :arg show_term_doc_count_error: Set to `true` to return the
        `doc_count_error_upper_bound`, which is an upper bound to the
        error on the `doc_count` returned by each shard.
    :arg size: The number of buckets returned out of the overall terms
        list. Defaults to `10` if omitted.
    :arg format:
    """

    name = "terms"

    def __init__(
        self,
        *,
        collect_mode: Union[
            Literal["depth_first", "breadth_first"], "DefaultType"
        ] = DEFAULT,
        exclude: Union[str, Sequence[str], "DefaultType"] = DEFAULT,
        execution_hint: Union[
            Literal[
                "map",
                "global_ordinals",
                "global_ordinals_hash",
                "global_ordinals_low_cardinality",
            ],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        include: Union[
            str, Sequence[str], "types.TermsPartition", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        missing_order: Union[
            Literal["first", "last", "default"], "DefaultType"
        ] = DEFAULT,
        missing_bucket: Union[bool, "DefaultType"] = DEFAULT,
        value_type: Union[str, "DefaultType"] = DEFAULT,
        order: Union[
            Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]],
            Sequence[Mapping[Union[str, "InstrumentedField"], Literal["asc", "desc"]]],
            "DefaultType",
        ] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        shard_min_doc_count: Union[int, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        show_term_doc_count_error: Union[bool, "DefaultType"] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            collect_mode=collect_mode,
            exclude=exclude,
            execution_hint=execution_hint,
            field=field,
            include=include,
            min_doc_count=min_doc_count,
            missing=missing,
            missing_order=missing_order,
            missing_bucket=missing_bucket,
            value_type=value_type,
            order=order,
            script=script,
            shard_min_doc_count=shard_min_doc_count,
            shard_size=shard_size,
            show_term_doc_count_error=show_term_doc_count_error,
            size=size,
            format=format,
            **kwargs,
        )

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return FieldBucketData(self, search, data)


class TimeSeries(Bucket[_R]):
    """
    The time series aggregation queries data created using a time series
    index. This is typically data such as metrics or other data streams
    with a time component, and requires creating an index using the time
    series mode.

    :arg size: The maximum number of results to return. Defaults to
        `10000` if omitted.
    :arg keyed: Set to `true` to associate a unique string key with each
        bucket and returns the ranges as a hash rather than an array.
    """

    name = "time_series"

    def __init__(
        self,
        *,
        size: Union[int, "DefaultType"] = DEFAULT,
        keyed: Union[bool, "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(size=size, keyed=keyed, **kwargs)


class TopHits(Agg[_R]):
    """
    A metric aggregation that returns the top matching documents per
    bucket.

    :arg docvalue_fields: Fields for which to return doc values.
    :arg explain: If `true`, returns detailed information about score
        computation as part of a hit.
    :arg fields: Array of wildcard (*) patterns. The request returns
        values for field names matching these patterns in the hits.fields
        property of the response.
    :arg from: Starting document offset.
    :arg highlight: Specifies the highlighter to use for retrieving
        highlighted snippets from one or more fields in the search
        results.
    :arg script_fields: Returns the result of one or more script
        evaluations for each hit.
    :arg size: The maximum number of top matching hits to return per
        bucket. Defaults to `3` if omitted.
    :arg sort: Sort order of the top matching hits. By default, the hits
        are sorted by the score of the main query.
    :arg _source: Selects the fields of the source that are returned.
    :arg stored_fields: Returns values for the specified stored fields
        (fields that use the `store` mapping option).
    :arg track_scores: If `true`, calculates and returns document scores,
        even if the scores are not used for sorting.
    :arg version: If `true`, returns document version as part of a hit.
    :arg seq_no_primary_term: If `true`, returns sequence number and
        primary term of the last modification of each hit.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "top_hits"

    def __init__(
        self,
        *,
        docvalue_fields: Union[
            Sequence["types.FieldAndFormat"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        explain: Union[bool, "DefaultType"] = DEFAULT,
        fields: Union[
            Sequence["types.FieldAndFormat"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        from_: Union[int, "DefaultType"] = DEFAULT,
        highlight: Union["types.Highlight", Dict[str, Any], "DefaultType"] = DEFAULT,
        script_fields: Union[
            Mapping[str, "types.ScriptField"], Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        sort: Union[
            Union[Union[str, "InstrumentedField"], "types.SortOptions"],
            Sequence[Union[Union[str, "InstrumentedField"], "types.SortOptions"]],
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        _source: Union[
            bool, "types.SourceFilter", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        stored_fields: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        track_scores: Union[bool, "DefaultType"] = DEFAULT,
        version: Union[bool, "DefaultType"] = DEFAULT,
        seq_no_primary_term: Union[bool, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            docvalue_fields=docvalue_fields,
            explain=explain,
            fields=fields,
            from_=from_,
            highlight=highlight,
            script_fields=script_fields,
            size=size,
            sort=sort,
            _source=_source,
            stored_fields=stored_fields,
            track_scores=track_scores,
            version=version,
            seq_no_primary_term=seq_no_primary_term,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return TopHitsData(self, search, data)


class TTest(Agg[_R]):
    """
    A metrics aggregation that performs a statistical hypothesis test in
    which the test statistic follows a Student’s t-distribution under the
    null hypothesis on numeric values extracted from the aggregated
    documents.

    :arg a: Test population A.
    :arg b: Test population B.
    :arg type: The type of test. Defaults to `heteroscedastic` if omitted.
    """

    name = "t_test"

    def __init__(
        self,
        *,
        a: Union["types.TestPopulation", Dict[str, Any], "DefaultType"] = DEFAULT,
        b: Union["types.TestPopulation", Dict[str, Any], "DefaultType"] = DEFAULT,
        type: Union[
            Literal["paired", "homoscedastic", "heteroscedastic"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(a=a, b=b, type=type, **kwargs)


class TopMetrics(Agg[_R]):
    """
    A metric aggregation that selects metrics from the document with the
    largest or smallest sort value.

    :arg metrics: The fields of the top document to return.
    :arg size: The number of top documents from which to return metrics.
        Defaults to `1` if omitted.
    :arg sort: The sort order of the documents.
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "top_metrics"

    def __init__(
        self,
        *,
        metrics: Union[
            "types.TopMetricsValue",
            Sequence["types.TopMetricsValue"],
            Sequence[Dict[str, Any]],
            "DefaultType",
        ] = DEFAULT,
        size: Union[int, "DefaultType"] = DEFAULT,
        sort: Union[
            Union[Union[str, "InstrumentedField"], "types.SortOptions"],
            Sequence[Union[Union[str, "InstrumentedField"], "types.SortOptions"]],
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            metrics=metrics,
            size=size,
            sort=sort,
            field=field,
            missing=missing,
            script=script,
            **kwargs,
        )


class ValueCount(Agg[_R]):
    """
    A single-value metrics aggregation that counts the number of values
    that are extracted from the aggregated documents.

    :arg format:
    :arg field: The field on which to run the aggregation.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    :arg script:
    """

    name = "value_count"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        missing: Union[str, int, float, bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, field=field, missing=missing, script=script, **kwargs
        )


class WeightedAvg(Agg[_R]):
    """
    A single-value metrics aggregation that computes the weighted average
    of numeric values that are extracted from the aggregated documents.

    :arg format: A numeric response formatter.
    :arg value: Configuration for the field that provides the values.
    :arg value_type:
    :arg weight: Configuration for the field or script that provides the
        weights.
    """

    name = "weighted_avg"

    def __init__(
        self,
        *,
        format: Union[str, "DefaultType"] = DEFAULT,
        value: Union[
            "types.WeightedAverageValue", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        value_type: Union[
            Literal[
                "string",
                "long",
                "double",
                "number",
                "date",
                "date_nanos",
                "ip",
                "numeric",
                "geo_point",
                "boolean",
            ],
            "DefaultType",
        ] = DEFAULT,
        weight: Union[
            "types.WeightedAverageValue", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            format=format, value=value, value_type=value_type, weight=weight, **kwargs
        )


class VariableWidthHistogram(Bucket[_R]):
    """
    A multi-bucket aggregation similar to the histogram, except instead of
    providing an interval to use as the width of each bucket, a target
    number of buckets is provided.

    :arg field: The name of the field.
    :arg buckets: The target number of buckets. Defaults to `10` if
        omitted.
    :arg shard_size: The number of buckets that the coordinating node will
        request from each shard. Defaults to `buckets * 50`.
    :arg initial_buffer: Specifies the number of individual documents that
        will be stored in memory on a shard before the initial bucketing
        algorithm is run. Defaults to `min(10 * shard_size, 50000)`.
    :arg script:
    """

    name = "variable_width_histogram"

    def __init__(
        self,
        *,
        field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        buckets: Union[int, "DefaultType"] = DEFAULT,
        shard_size: Union[int, "DefaultType"] = DEFAULT,
        initial_buffer: Union[int, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        **kwargs: Any,
    ):
        super().__init__(
            field=field,
            buckets=buckets,
            shard_size=shard_size,
            initial_buffer=initial_buffer,
            script=script,
            **kwargs,
        )

    def result(self, search: "SearchBase[_R]", data: Any) -> AttrDict[Any]:
        return FieldBucketData(self, search, data)
