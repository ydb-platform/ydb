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

from typing import Any, Dict, Literal, Mapping, Sequence, Union

from elastic_transport.client_utils import DEFAULT, DefaultType

from . import Query
from .document_base import InstrumentedField
from .utils import AttrDict

PipeSeparatedFlags = str


class BucketCorrelationFunction(AttrDict[Any]):
    """
    :arg count_correlation: (required) The configuration to calculate a
        count correlation. This function is designed for determining the
        correlation of a term value and a given metric.
    """

    count_correlation: Union[
        "BucketCorrelationFunctionCountCorrelation", Dict[str, Any], DefaultType
    ]

    def __init__(
        self,
        *,
        count_correlation: Union[
            "BucketCorrelationFunctionCountCorrelation", Dict[str, Any], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if count_correlation is not DEFAULT:
            kwargs["count_correlation"] = count_correlation
        super().__init__(kwargs)


class BucketCorrelationFunctionCountCorrelation(AttrDict[Any]):
    """
    :arg indicator: (required) The indicator with which to correlate the
        configured `bucket_path` values.
    """

    indicator: Union[
        "BucketCorrelationFunctionCountCorrelationIndicator",
        Dict[str, Any],
        DefaultType,
    ]

    def __init__(
        self,
        *,
        indicator: Union[
            "BucketCorrelationFunctionCountCorrelationIndicator",
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if indicator is not DEFAULT:
            kwargs["indicator"] = indicator
        super().__init__(kwargs)


class BucketCorrelationFunctionCountCorrelationIndicator(AttrDict[Any]):
    """
    :arg doc_count: (required) The total number of documents that
        initially created the expectations. It’s required to be greater
        than or equal to the sum of all values in the buckets_path as this
        is the originating superset of data to which the term values are
        correlated.
    :arg expectations: (required) An array of numbers with which to
        correlate the configured `bucket_path` values. The length of this
        value must always equal the number of buckets returned by the
        `bucket_path`.
    :arg fractions: An array of fractions to use when averaging and
        calculating variance. This should be used if the pre-calculated
        data and the buckets_path have known gaps. The length of
        fractions, if provided, must equal expectations.
    """

    doc_count: Union[int, DefaultType]
    expectations: Union[Sequence[float], DefaultType]
    fractions: Union[Sequence[float], DefaultType]

    def __init__(
        self,
        *,
        doc_count: Union[int, DefaultType] = DEFAULT,
        expectations: Union[Sequence[float], DefaultType] = DEFAULT,
        fractions: Union[Sequence[float], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if doc_count is not DEFAULT:
            kwargs["doc_count"] = doc_count
        if expectations is not DEFAULT:
            kwargs["expectations"] = expectations
        if fractions is not DEFAULT:
            kwargs["fractions"] = fractions
        super().__init__(kwargs)


class ChiSquareHeuristic(AttrDict[Any]):
    """
    :arg background_is_superset: (required) Set to `false` if you defined
        a custom background filter that represents a different set of
        documents that you want to compare to.
    :arg include_negatives: (required) Set to `false` to filter out the
        terms that appear less often in the subset than in documents
        outside the subset.
    """

    background_is_superset: Union[bool, DefaultType]
    include_negatives: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        background_is_superset: Union[bool, DefaultType] = DEFAULT,
        include_negatives: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if background_is_superset is not DEFAULT:
            kwargs["background_is_superset"] = background_is_superset
        if include_negatives is not DEFAULT:
            kwargs["include_negatives"] = include_negatives
        super().__init__(kwargs)


class ChunkingSettings(AttrDict[Any]):
    """
    :arg strategy: (required) The chunking strategy: `sentence`, `word`,
        `none` or `recursive`.   * If `strategy` is set to `recursive`,
        you must also specify:  - `max_chunk_size` - either `separators`
        or`separator_group`  Learn more about different chunking
        strategies in the linked documentation. Defaults to `sentence` if
        omitted.
    :arg max_chunk_size: (required) The maximum size of a chunk in words.
        This value cannot be higher than `300` or lower than `20` (for
        `sentence` strategy) or `10` (for `word` strategy). Defaults to
        `250` if omitted.
    :arg separator_group: Only applicable to the `recursive` strategy and
        required when using it.  Sets a predefined list of separators in
        the saved chunking settings based on the selected text type.
        Values can be `markdown` or `plaintext`.  Using this parameter is
        an alternative to manually specifying a custom `separators` list.
    :arg separators: Only applicable to the `recursive` strategy and
        required when using it.  A list of strings used as possible split
        points when chunking text.  Each string can be a plain string or a
        regular expression (regex) pattern. The system tries each
        separator in order to split the text, starting from the first item
        in the list.  After splitting, it attempts to recombine smaller
        pieces into larger chunks that stay within the `max_chunk_size`
        limit, to reduce the total number of chunks generated.
    :arg overlap: The number of overlapping words for chunks. It is
        applicable only to a `word` chunking strategy. This value cannot
        be higher than half the `max_chunk_size` value. Defaults to `100`
        if omitted.
    :arg sentence_overlap: The number of overlapping sentences for chunks.
        It is applicable only for a `sentence` chunking strategy. It can
        be either `1` or `0`. Defaults to `1` if omitted.
    """

    strategy: Union[str, DefaultType]
    max_chunk_size: Union[int, DefaultType]
    separator_group: Union[str, DefaultType]
    separators: Union[Sequence[str], DefaultType]
    overlap: Union[int, DefaultType]
    sentence_overlap: Union[int, DefaultType]

    def __init__(
        self,
        *,
        strategy: Union[str, DefaultType] = DEFAULT,
        max_chunk_size: Union[int, DefaultType] = DEFAULT,
        separator_group: Union[str, DefaultType] = DEFAULT,
        separators: Union[Sequence[str], DefaultType] = DEFAULT,
        overlap: Union[int, DefaultType] = DEFAULT,
        sentence_overlap: Union[int, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if strategy is not DEFAULT:
            kwargs["strategy"] = strategy
        if max_chunk_size is not DEFAULT:
            kwargs["max_chunk_size"] = max_chunk_size
        if separator_group is not DEFAULT:
            kwargs["separator_group"] = separator_group
        if separators is not DEFAULT:
            kwargs["separators"] = separators
        if overlap is not DEFAULT:
            kwargs["overlap"] = overlap
        if sentence_overlap is not DEFAULT:
            kwargs["sentence_overlap"] = sentence_overlap
        super().__init__(kwargs)


class ClassificationInferenceOptions(AttrDict[Any]):
    """
    :arg num_top_classes: Specifies the number of top class predictions to
        return. Defaults to 0.
    :arg num_top_feature_importance_values: Specifies the maximum number
        of feature importance values per document.
    :arg prediction_field_type: Specifies the type of the predicted field
        to write. Acceptable values are: string, number, boolean. When
        boolean is provided 1.0 is transformed to true and 0.0 to false.
    :arg results_field: The field that is added to incoming documents to
        contain the inference prediction. Defaults to predicted_value.
    :arg top_classes_results_field: Specifies the field to which the top
        classes are written. Defaults to top_classes.
    """

    num_top_classes: Union[int, DefaultType]
    num_top_feature_importance_values: Union[int, DefaultType]
    prediction_field_type: Union[str, DefaultType]
    results_field: Union[str, DefaultType]
    top_classes_results_field: Union[str, DefaultType]

    def __init__(
        self,
        *,
        num_top_classes: Union[int, DefaultType] = DEFAULT,
        num_top_feature_importance_values: Union[int, DefaultType] = DEFAULT,
        prediction_field_type: Union[str, DefaultType] = DEFAULT,
        results_field: Union[str, DefaultType] = DEFAULT,
        top_classes_results_field: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if num_top_classes is not DEFAULT:
            kwargs["num_top_classes"] = num_top_classes
        if num_top_feature_importance_values is not DEFAULT:
            kwargs["num_top_feature_importance_values"] = (
                num_top_feature_importance_values
            )
        if prediction_field_type is not DEFAULT:
            kwargs["prediction_field_type"] = prediction_field_type
        if results_field is not DEFAULT:
            kwargs["results_field"] = results_field
        if top_classes_results_field is not DEFAULT:
            kwargs["top_classes_results_field"] = top_classes_results_field
        super().__init__(kwargs)


class CommonTermsQuery(AttrDict[Any]):
    """
    :arg query: (required)
    :arg analyzer:
    :arg cutoff_frequency:
    :arg high_freq_operator:
    :arg low_freq_operator:
    :arg minimum_should_match:
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    query: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    cutoff_frequency: Union[float, DefaultType]
    high_freq_operator: Union[Literal["and", "or"], DefaultType]
    low_freq_operator: Union[Literal["and", "or"], DefaultType]
    minimum_should_match: Union[int, str, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        query: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        cutoff_frequency: Union[float, DefaultType] = DEFAULT,
        high_freq_operator: Union[Literal["and", "or"], DefaultType] = DEFAULT,
        low_freq_operator: Union[Literal["and", "or"], DefaultType] = DEFAULT,
        minimum_should_match: Union[int, str, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if query is not DEFAULT:
            kwargs["query"] = query
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if cutoff_frequency is not DEFAULT:
            kwargs["cutoff_frequency"] = cutoff_frequency
        if high_freq_operator is not DEFAULT:
            kwargs["high_freq_operator"] = high_freq_operator
        if low_freq_operator is not DEFAULT:
            kwargs["low_freq_operator"] = low_freq_operator
        if minimum_should_match is not DEFAULT:
            kwargs["minimum_should_match"] = minimum_should_match
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class CoordsGeoBounds(AttrDict[Any]):
    """
    :arg top: (required)
    :arg bottom: (required)
    :arg left: (required)
    :arg right: (required)
    """

    top: Union[float, DefaultType]
    bottom: Union[float, DefaultType]
    left: Union[float, DefaultType]
    right: Union[float, DefaultType]

    def __init__(
        self,
        *,
        top: Union[float, DefaultType] = DEFAULT,
        bottom: Union[float, DefaultType] = DEFAULT,
        left: Union[float, DefaultType] = DEFAULT,
        right: Union[float, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if top is not DEFAULT:
            kwargs["top"] = top
        if bottom is not DEFAULT:
            kwargs["bottom"] = bottom
        if left is not DEFAULT:
            kwargs["left"] = left
        if right is not DEFAULT:
            kwargs["right"] = right
        super().__init__(kwargs)


class CustomCategorizeTextAnalyzer(AttrDict[Any]):
    """
    :arg char_filter:
    :arg tokenizer:
    :arg filter:
    """

    char_filter: Union[Sequence[str], DefaultType]
    tokenizer: Union[str, DefaultType]
    filter: Union[Sequence[str], DefaultType]

    def __init__(
        self,
        *,
        char_filter: Union[Sequence[str], DefaultType] = DEFAULT,
        tokenizer: Union[str, DefaultType] = DEFAULT,
        filter: Union[Sequence[str], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if char_filter is not DEFAULT:
            kwargs["char_filter"] = char_filter
        if tokenizer is not DEFAULT:
            kwargs["tokenizer"] = tokenizer
        if filter is not DEFAULT:
            kwargs["filter"] = filter
        super().__init__(kwargs)


class DenseVectorIndexOptions(AttrDict[Any]):
    """
    :arg type: (required) The type of kNN algorithm to use.
    :arg confidence_interval: The confidence interval to use when
        quantizing the vectors. Can be any value between and including
        `0.90` and `1.0` or exactly `0`. When the value is `0`, this
        indicates that dynamic quantiles should be calculated for
        optimized quantization. When between `0.90` and `1.0`, this value
        restricts the values used when calculating the quantization
        thresholds.  For example, a value of `0.95` will only use the
        middle `95%` of the values when calculating the quantization
        thresholds (e.g. the highest and lowest `2.5%` of values will be
        ignored).  Defaults to `1/(dims + 1)` for `int8` quantized vectors
        and `0` for `int4` for dynamic quantile calculation.  Only
        applicable to `int8_hnsw`, `int4_hnsw`, `int8_flat`, and
        `int4_flat` index types.
    :arg ef_construction: The number of candidates to track while
        assembling the list of nearest neighbors for each new node.  Only
        applicable to `hnsw`, `int8_hnsw`, `bbq_hnsw`, and `int4_hnsw`
        index types. Defaults to `100` if omitted.
    :arg m: The number of neighbors each node will be connected to in the
        HNSW graph.  Only applicable to `hnsw`, `int8_hnsw`, `bbq_hnsw`,
        and `int4_hnsw` index types. Defaults to `16` if omitted.
    :arg rescore_vector: The rescore vector options. This is only
        applicable to `bbq_hnsw`, `int4_hnsw`, `int8_hnsw`, `bbq_flat`,
        `int4_flat`, and `int8_flat` index types.
    """

    type: Union[
        Literal[
            "bbq_flat",
            "bbq_hnsw",
            "flat",
            "hnsw",
            "int4_flat",
            "int4_hnsw",
            "int8_flat",
            "int8_hnsw",
        ],
        DefaultType,
    ]
    confidence_interval: Union[float, DefaultType]
    ef_construction: Union[int, DefaultType]
    m: Union[int, DefaultType]
    rescore_vector: Union[
        "DenseVectorIndexOptionsRescoreVector", Dict[str, Any], DefaultType
    ]

    def __init__(
        self,
        *,
        type: Union[
            Literal[
                "bbq_flat",
                "bbq_hnsw",
                "flat",
                "hnsw",
                "int4_flat",
                "int4_hnsw",
                "int8_flat",
                "int8_hnsw",
            ],
            DefaultType,
        ] = DEFAULT,
        confidence_interval: Union[float, DefaultType] = DEFAULT,
        ef_construction: Union[int, DefaultType] = DEFAULT,
        m: Union[int, DefaultType] = DEFAULT,
        rescore_vector: Union[
            "DenseVectorIndexOptionsRescoreVector", Dict[str, Any], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if type is not DEFAULT:
            kwargs["type"] = type
        if confidence_interval is not DEFAULT:
            kwargs["confidence_interval"] = confidence_interval
        if ef_construction is not DEFAULT:
            kwargs["ef_construction"] = ef_construction
        if m is not DEFAULT:
            kwargs["m"] = m
        if rescore_vector is not DEFAULT:
            kwargs["rescore_vector"] = rescore_vector
        super().__init__(kwargs)


class DenseVectorIndexOptionsRescoreVector(AttrDict[Any]):
    """
    :arg oversample: (required) The oversampling factor to use when
        searching for the nearest neighbor. This is only applicable to the
        quantized formats: `bbq_*`, `int4_*`, and `int8_*`. When provided,
        `oversample * k` vectors will be gathered and then their scores
        will be re-computed with the original vectors.  valid values are
        between `1.0` and `10.0` (inclusive), or `0` exactly to disable
        oversampling.
    """

    oversample: Union[float, DefaultType]

    def __init__(
        self, *, oversample: Union[float, DefaultType] = DEFAULT, **kwargs: Any
    ):
        if oversample is not DEFAULT:
            kwargs["oversample"] = oversample
        super().__init__(kwargs)


class EmptyObject(AttrDict[Any]):
    """
    For empty Class assignments
    """

    def __init__(self, **kwargs: Any):
        super().__init__(kwargs)


class EwmaModelSettings(AttrDict[Any]):
    """
    :arg alpha:
    """

    alpha: Union[float, DefaultType]

    def __init__(self, *, alpha: Union[float, DefaultType] = DEFAULT, **kwargs: Any):
        if alpha is not DEFAULT:
            kwargs["alpha"] = alpha
        super().__init__(kwargs)


class ExtendedBounds(AttrDict[Any]):
    """
    :arg max: Maximum value for the bound.
    :arg min: Minimum value for the bound.
    """

    max: Any
    min: Any

    def __init__(self, *, max: Any = DEFAULT, min: Any = DEFAULT, **kwargs: Any):
        if max is not DEFAULT:
            kwargs["max"] = max
        if min is not DEFAULT:
            kwargs["min"] = min
        super().__init__(kwargs)


class FieldAndFormat(AttrDict[Any]):
    """
    A reference to a field with formatting instructions on how to return
    the value

    :arg field: (required) A wildcard pattern. The request returns values
        for field names matching this pattern.
    :arg format: The format in which the values are returned.
    :arg include_unmapped:
    """

    field: Union[str, InstrumentedField, DefaultType]
    format: Union[str, DefaultType]
    include_unmapped: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        format: Union[str, DefaultType] = DEFAULT,
        include_unmapped: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if format is not DEFAULT:
            kwargs["format"] = format
        if include_unmapped is not DEFAULT:
            kwargs["include_unmapped"] = include_unmapped
        super().__init__(kwargs)


class FieldCollapse(AttrDict[Any]):
    """
    :arg field: (required) The field to collapse the result set on
    :arg inner_hits: The number of inner hits and their sort order
    :arg max_concurrent_group_searches: The number of concurrent requests
        allowed to retrieve the inner_hits per group
    :arg collapse:
    """

    field: Union[str, InstrumentedField, DefaultType]
    inner_hits: Union[
        "InnerHits", Sequence["InnerHits"], Sequence[Dict[str, Any]], DefaultType
    ]
    max_concurrent_group_searches: Union[int, DefaultType]
    collapse: Union["FieldCollapse", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        inner_hits: Union[
            "InnerHits", Sequence["InnerHits"], Sequence[Dict[str, Any]], DefaultType
        ] = DEFAULT,
        max_concurrent_group_searches: Union[int, DefaultType] = DEFAULT,
        collapse: Union["FieldCollapse", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if inner_hits is not DEFAULT:
            kwargs["inner_hits"] = inner_hits
        if max_concurrent_group_searches is not DEFAULT:
            kwargs["max_concurrent_group_searches"] = max_concurrent_group_searches
        if collapse is not DEFAULT:
            kwargs["collapse"] = collapse
        super().__init__(kwargs)


class FieldLookup(AttrDict[Any]):
    """
    :arg id: (required) `id` of the document.
    :arg index: Index from which to retrieve the document.
    :arg path: Name of the field.
    :arg routing: Custom routing value.
    """

    id: Union[str, DefaultType]
    index: Union[str, DefaultType]
    path: Union[str, InstrumentedField, DefaultType]
    routing: Union[str, DefaultType]

    def __init__(
        self,
        *,
        id: Union[str, DefaultType] = DEFAULT,
        index: Union[str, DefaultType] = DEFAULT,
        path: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        routing: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if id is not DEFAULT:
            kwargs["id"] = id
        if index is not DEFAULT:
            kwargs["index"] = index
        if path is not DEFAULT:
            kwargs["path"] = str(path)
        if routing is not DEFAULT:
            kwargs["routing"] = routing
        super().__init__(kwargs)


class FieldSort(AttrDict[Any]):
    """
    :arg missing:
    :arg mode:
    :arg nested:
    :arg order:
    :arg unmapped_type:
    :arg numeric_type:
    :arg format:
    """

    missing: Union[str, int, float, bool, DefaultType]
    mode: Union[Literal["min", "max", "sum", "avg", "median"], DefaultType]
    nested: Union["NestedSortValue", Dict[str, Any], DefaultType]
    order: Union[Literal["asc", "desc"], DefaultType]
    unmapped_type: Union[
        Literal[
            "none",
            "geo_point",
            "geo_shape",
            "ip",
            "binary",
            "keyword",
            "text",
            "search_as_you_type",
            "date",
            "date_nanos",
            "boolean",
            "completion",
            "nested",
            "object",
            "passthrough",
            "version",
            "murmur3",
            "token_count",
            "percolator",
            "integer",
            "long",
            "short",
            "byte",
            "float",
            "half_float",
            "scaled_float",
            "double",
            "integer_range",
            "float_range",
            "long_range",
            "double_range",
            "date_range",
            "ip_range",
            "alias",
            "join",
            "rank_feature",
            "rank_features",
            "flattened",
            "shape",
            "histogram",
            "constant_keyword",
            "counted_keyword",
            "aggregate_metric_double",
            "dense_vector",
            "semantic_text",
            "sparse_vector",
            "match_only_text",
            "icu_collation_keyword",
        ],
        DefaultType,
    ]
    numeric_type: Union[Literal["long", "double", "date", "date_nanos"], DefaultType]
    format: Union[str, DefaultType]

    def __init__(
        self,
        *,
        missing: Union[str, int, float, bool, DefaultType] = DEFAULT,
        mode: Union[
            Literal["min", "max", "sum", "avg", "median"], DefaultType
        ] = DEFAULT,
        nested: Union["NestedSortValue", Dict[str, Any], DefaultType] = DEFAULT,
        order: Union[Literal["asc", "desc"], DefaultType] = DEFAULT,
        unmapped_type: Union[
            Literal[
                "none",
                "geo_point",
                "geo_shape",
                "ip",
                "binary",
                "keyword",
                "text",
                "search_as_you_type",
                "date",
                "date_nanos",
                "boolean",
                "completion",
                "nested",
                "object",
                "passthrough",
                "version",
                "murmur3",
                "token_count",
                "percolator",
                "integer",
                "long",
                "short",
                "byte",
                "float",
                "half_float",
                "scaled_float",
                "double",
                "integer_range",
                "float_range",
                "long_range",
                "double_range",
                "date_range",
                "ip_range",
                "alias",
                "join",
                "rank_feature",
                "rank_features",
                "flattened",
                "shape",
                "histogram",
                "constant_keyword",
                "counted_keyword",
                "aggregate_metric_double",
                "dense_vector",
                "semantic_text",
                "sparse_vector",
                "match_only_text",
                "icu_collation_keyword",
            ],
            DefaultType,
        ] = DEFAULT,
        numeric_type: Union[
            Literal["long", "double", "date", "date_nanos"], DefaultType
        ] = DEFAULT,
        format: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if missing is not DEFAULT:
            kwargs["missing"] = missing
        if mode is not DEFAULT:
            kwargs["mode"] = mode
        if nested is not DEFAULT:
            kwargs["nested"] = nested
        if order is not DEFAULT:
            kwargs["order"] = order
        if unmapped_type is not DEFAULT:
            kwargs["unmapped_type"] = unmapped_type
        if numeric_type is not DEFAULT:
            kwargs["numeric_type"] = numeric_type
        if format is not DEFAULT:
            kwargs["format"] = format
        super().__init__(kwargs)


class FielddataFrequencyFilter(AttrDict[Any]):
    """
    :arg max: (required)
    :arg min: (required)
    :arg min_segment_size: (required)
    """

    max: Union[float, DefaultType]
    min: Union[float, DefaultType]
    min_segment_size: Union[int, DefaultType]

    def __init__(
        self,
        *,
        max: Union[float, DefaultType] = DEFAULT,
        min: Union[float, DefaultType] = DEFAULT,
        min_segment_size: Union[int, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if max is not DEFAULT:
            kwargs["max"] = max
        if min is not DEFAULT:
            kwargs["min"] = min
        if min_segment_size is not DEFAULT:
            kwargs["min_segment_size"] = min_segment_size
        super().__init__(kwargs)


class FrequentItemSetsField(AttrDict[Any]):
    """
    :arg field: (required)
    :arg exclude: Values to exclude. Can be regular expression strings or
        arrays of strings of exact terms.
    :arg include: Values to include. Can be regular expression strings or
        arrays of strings of exact terms.
    """

    field: Union[str, InstrumentedField, DefaultType]
    exclude: Union[str, Sequence[str], DefaultType]
    include: Union[str, Sequence[str], "TermsPartition", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        exclude: Union[str, Sequence[str], DefaultType] = DEFAULT,
        include: Union[
            str, Sequence[str], "TermsPartition", Dict[str, Any], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if exclude is not DEFAULT:
            kwargs["exclude"] = exclude
        if include is not DEFAULT:
            kwargs["include"] = include
        super().__init__(kwargs)


class FuzzyQuery(AttrDict[Any]):
    """
    :arg value: (required) Term you wish to find in the provided field.
    :arg max_expansions: Maximum number of variations created. Defaults to
        `50` if omitted.
    :arg prefix_length: Number of beginning characters left unchanged when
        creating expansions.
    :arg rewrite: Number of beginning characters left unchanged when
        creating expansions. Defaults to `constant_score` if omitted.
    :arg transpositions: Indicates whether edits include transpositions of
        two adjacent characters (for example `ab` to `ba`). Defaults to
        `True` if omitted.
    :arg fuzziness: Maximum edit distance allowed for matching.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    value: Union[str, float, bool, DefaultType]
    max_expansions: Union[int, DefaultType]
    prefix_length: Union[int, DefaultType]
    rewrite: Union[str, DefaultType]
    transpositions: Union[bool, DefaultType]
    fuzziness: Union[str, int, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        value: Union[str, float, bool, DefaultType] = DEFAULT,
        max_expansions: Union[int, DefaultType] = DEFAULT,
        prefix_length: Union[int, DefaultType] = DEFAULT,
        rewrite: Union[str, DefaultType] = DEFAULT,
        transpositions: Union[bool, DefaultType] = DEFAULT,
        fuzziness: Union[str, int, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if value is not DEFAULT:
            kwargs["value"] = value
        if max_expansions is not DEFAULT:
            kwargs["max_expansions"] = max_expansions
        if prefix_length is not DEFAULT:
            kwargs["prefix_length"] = prefix_length
        if rewrite is not DEFAULT:
            kwargs["rewrite"] = rewrite
        if transpositions is not DEFAULT:
            kwargs["transpositions"] = transpositions
        if fuzziness is not DEFAULT:
            kwargs["fuzziness"] = fuzziness
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class GeoDistanceSort(AttrDict[Any]):
    """
    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg mode:
    :arg distance_type:
    :arg ignore_unmapped:
    :arg order:
    :arg unit:
    :arg nested:
    """

    _field: Union[str, "InstrumentedField", "DefaultType"]
    _value: Union[
        Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str],
        Sequence[Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str]],
        Dict[str, Any],
        "DefaultType",
    ]
    mode: Union[Literal["min", "max", "sum", "avg", "median"], DefaultType]
    distance_type: Union[Literal["arc", "plane"], DefaultType]
    ignore_unmapped: Union[bool, DefaultType]
    order: Union[Literal["asc", "desc"], DefaultType]
    unit: Union[
        Literal["in", "ft", "yd", "mi", "nmi", "km", "m", "cm", "mm"], DefaultType
    ]
    nested: Union["NestedSortValue", Dict[str, Any], DefaultType]

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union[
            Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str],
            Sequence[
                Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str]
            ],
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        *,
        mode: Union[
            Literal["min", "max", "sum", "avg", "median"], DefaultType
        ] = DEFAULT,
        distance_type: Union[Literal["arc", "plane"], DefaultType] = DEFAULT,
        ignore_unmapped: Union[bool, DefaultType] = DEFAULT,
        order: Union[Literal["asc", "desc"], DefaultType] = DEFAULT,
        unit: Union[
            Literal["in", "ft", "yd", "mi", "nmi", "km", "m", "cm", "mm"], DefaultType
        ] = DEFAULT,
        nested: Union["NestedSortValue", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        if mode is not DEFAULT:
            kwargs["mode"] = mode
        if distance_type is not DEFAULT:
            kwargs["distance_type"] = distance_type
        if ignore_unmapped is not DEFAULT:
            kwargs["ignore_unmapped"] = ignore_unmapped
        if order is not DEFAULT:
            kwargs["order"] = order
        if unit is not DEFAULT:
            kwargs["unit"] = unit
        if nested is not DEFAULT:
            kwargs["nested"] = nested
        super().__init__(kwargs)


class GeoGridQuery(AttrDict[Any]):
    """
    :arg geotile:
    :arg geohash:
    :arg geohex:
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    geotile: Union[str, DefaultType]
    geohash: Union[str, DefaultType]
    geohex: Union[str, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        geotile: Union[str, DefaultType] = DEFAULT,
        geohash: Union[str, DefaultType] = DEFAULT,
        geohex: Union[str, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if geotile is not DEFAULT:
            kwargs["geotile"] = geotile
        if geohash is not DEFAULT:
            kwargs["geohash"] = geohash
        if geohex is not DEFAULT:
            kwargs["geohex"] = geohex
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class GeoHashLocation(AttrDict[Any]):
    """
    :arg geohash: (required)
    """

    geohash: Union[str, DefaultType]

    def __init__(self, *, geohash: Union[str, DefaultType] = DEFAULT, **kwargs: Any):
        if geohash is not DEFAULT:
            kwargs["geohash"] = geohash
        super().__init__(kwargs)


class GeoLinePoint(AttrDict[Any]):
    """
    :arg field: (required) The name of the geo_point field.
    """

    field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        super().__init__(kwargs)


class GeoLineSort(AttrDict[Any]):
    """
    :arg field: (required) The name of the numeric field to use as the
        sort key for ordering the points.
    """

    field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        super().__init__(kwargs)


class GeoPolygonPoints(AttrDict[Any]):
    """
    :arg points: (required)
    """

    points: Union[
        Sequence[Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str]],
        Dict[str, Any],
        DefaultType,
    ]

    def __init__(
        self,
        *,
        points: Union[
            Sequence[
                Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str]
            ],
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if points is not DEFAULT:
            kwargs["points"] = points
        super().__init__(kwargs)


class GeoShapeFieldQuery(AttrDict[Any]):
    """
    :arg shape:
    :arg indexed_shape: Query using an indexed shape retrieved from the
        the specified document and path.
    :arg relation: Spatial relation operator used to search a geo field.
        Defaults to `intersects` if omitted.
    """

    shape: Any
    indexed_shape: Union["FieldLookup", Dict[str, Any], DefaultType]
    relation: Union[
        Literal["intersects", "disjoint", "within", "contains"], DefaultType
    ]

    def __init__(
        self,
        *,
        shape: Any = DEFAULT,
        indexed_shape: Union["FieldLookup", Dict[str, Any], DefaultType] = DEFAULT,
        relation: Union[
            Literal["intersects", "disjoint", "within", "contains"], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if shape is not DEFAULT:
            kwargs["shape"] = shape
        if indexed_shape is not DEFAULT:
            kwargs["indexed_shape"] = indexed_shape
        if relation is not DEFAULT:
            kwargs["relation"] = relation
        super().__init__(kwargs)


class GoogleNormalizedDistanceHeuristic(AttrDict[Any]):
    """
    :arg background_is_superset: Set to `false` if you defined a custom
        background filter that represents a different set of documents
        that you want to compare to.
    """

    background_is_superset: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        background_is_superset: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if background_is_superset is not DEFAULT:
            kwargs["background_is_superset"] = background_is_superset
        super().__init__(kwargs)


class HdrMethod(AttrDict[Any]):
    """
    :arg number_of_significant_value_digits: Specifies the resolution of
        values for the histogram in number of significant digits.
    """

    number_of_significant_value_digits: Union[int, DefaultType]

    def __init__(
        self,
        *,
        number_of_significant_value_digits: Union[int, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if number_of_significant_value_digits is not DEFAULT:
            kwargs["number_of_significant_value_digits"] = (
                number_of_significant_value_digits
            )
        super().__init__(kwargs)


class Highlight(AttrDict[Any]):
    """
    :arg fields: (required)
    :arg encoder:
    :arg type:
    :arg boundary_chars: A string that contains each boundary character.
        Defaults to `.,!? \t\n` if omitted.
    :arg boundary_max_scan: How far to scan for boundary characters.
        Defaults to `20` if omitted.
    :arg boundary_scanner: Specifies how to break the highlighted
        fragments: chars, sentence, or word. Only valid for the unified
        and fvh highlighters. Defaults to `sentence` for the `unified`
        highlighter. Defaults to `chars` for the `fvh` highlighter.
    :arg boundary_scanner_locale: Controls which locale is used to search
        for sentence and word boundaries. This parameter takes a form of a
        language tag, for example: `"en-US"`, `"fr-FR"`, `"ja-JP"`.
        Defaults to `Locale.ROOT` if omitted.
    :arg force_source:
    :arg fragmenter: Specifies how text should be broken up in highlight
        snippets: `simple` or `span`. Only valid for the `plain`
        highlighter. Defaults to `span` if omitted.
    :arg fragment_size: The size of the highlighted fragment in
        characters. Defaults to `100` if omitted.
    :arg highlight_filter:
    :arg highlight_query: Highlight matches for a query other than the
        search query. This is especially useful if you use a rescore query
        because those are not taken into account by highlighting by
        default.
    :arg max_fragment_length:
    :arg max_analyzed_offset: If set to a non-negative value, highlighting
        stops at this defined maximum limit. The rest of the text is not
        processed, thus not highlighted and no error is returned The
        `max_analyzed_offset` query setting does not override the
        `index.highlight.max_analyzed_offset` setting, which prevails when
        it’s set to lower value than the query setting.
    :arg no_match_size: The amount of text you want to return from the
        beginning of the field if there are no matching fragments to
        highlight.
    :arg number_of_fragments: The maximum number of fragments to return.
        If the number of fragments is set to `0`, no fragments are
        returned. Instead, the entire field contents are highlighted and
        returned. This can be handy when you need to highlight short texts
        such as a title or address, but fragmentation is not required. If
        `number_of_fragments` is `0`, `fragment_size` is ignored. Defaults
        to `5` if omitted.
    :arg options:
    :arg order: Sorts highlighted fragments by score when set to `score`.
        By default, fragments will be output in the order they appear in
        the field (order: `none`). Setting this option to `score` will
        output the most relevant fragments first. Each highlighter applies
        its own logic to compute relevancy scores. Defaults to `none` if
        omitted.
    :arg phrase_limit: Controls the number of matching phrases in a
        document that are considered. Prevents the `fvh` highlighter from
        analyzing too many phrases and consuming too much memory. When
        using `matched_fields`, `phrase_limit` phrases per matched field
        are considered. Raising the limit increases query time and
        consumes more memory. Only supported by the `fvh` highlighter.
        Defaults to `256` if omitted.
    :arg post_tags: Use in conjunction with `pre_tags` to define the HTML
        tags to use for the highlighted text. By default, highlighted text
        is wrapped in `<em>` and `</em>` tags.
    :arg pre_tags: Use in conjunction with `post_tags` to define the HTML
        tags to use for the highlighted text. By default, highlighted text
        is wrapped in `<em>` and `</em>` tags.
    :arg require_field_match: By default, only fields that contains a
        query match are highlighted. Set to `false` to highlight all
        fields. Defaults to `True` if omitted.
    :arg tags_schema: Set to `styled` to use the built-in tag schema.
    """

    fields: Union[
        Mapping[Union[str, InstrumentedField], "HighlightField"],
        Sequence[Mapping[Union[str, InstrumentedField], "HighlightField"]],
        Dict[str, Any],
        DefaultType,
    ]
    encoder: Union[Literal["default", "html"], DefaultType]
    type: Union[Literal["plain", "fvh", "unified"], DefaultType]
    boundary_chars: Union[str, DefaultType]
    boundary_max_scan: Union[int, DefaultType]
    boundary_scanner: Union[Literal["chars", "sentence", "word"], DefaultType]
    boundary_scanner_locale: Union[str, DefaultType]
    force_source: Union[bool, DefaultType]
    fragmenter: Union[Literal["simple", "span"], DefaultType]
    fragment_size: Union[int, DefaultType]
    highlight_filter: Union[bool, DefaultType]
    highlight_query: Union[Query, DefaultType]
    max_fragment_length: Union[int, DefaultType]
    max_analyzed_offset: Union[int, DefaultType]
    no_match_size: Union[int, DefaultType]
    number_of_fragments: Union[int, DefaultType]
    options: Union[Mapping[str, Any], DefaultType]
    order: Union[Literal["score"], DefaultType]
    phrase_limit: Union[int, DefaultType]
    post_tags: Union[Sequence[str], DefaultType]
    pre_tags: Union[Sequence[str], DefaultType]
    require_field_match: Union[bool, DefaultType]
    tags_schema: Union[Literal["styled"], DefaultType]

    def __init__(
        self,
        *,
        fields: Union[
            Mapping[Union[str, InstrumentedField], "HighlightField"],
            Sequence[Mapping[Union[str, InstrumentedField], "HighlightField"]],
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        encoder: Union[Literal["default", "html"], DefaultType] = DEFAULT,
        type: Union[Literal["plain", "fvh", "unified"], DefaultType] = DEFAULT,
        boundary_chars: Union[str, DefaultType] = DEFAULT,
        boundary_max_scan: Union[int, DefaultType] = DEFAULT,
        boundary_scanner: Union[
            Literal["chars", "sentence", "word"], DefaultType
        ] = DEFAULT,
        boundary_scanner_locale: Union[str, DefaultType] = DEFAULT,
        force_source: Union[bool, DefaultType] = DEFAULT,
        fragmenter: Union[Literal["simple", "span"], DefaultType] = DEFAULT,
        fragment_size: Union[int, DefaultType] = DEFAULT,
        highlight_filter: Union[bool, DefaultType] = DEFAULT,
        highlight_query: Union[Query, DefaultType] = DEFAULT,
        max_fragment_length: Union[int, DefaultType] = DEFAULT,
        max_analyzed_offset: Union[int, DefaultType] = DEFAULT,
        no_match_size: Union[int, DefaultType] = DEFAULT,
        number_of_fragments: Union[int, DefaultType] = DEFAULT,
        options: Union[Mapping[str, Any], DefaultType] = DEFAULT,
        order: Union[Literal["score"], DefaultType] = DEFAULT,
        phrase_limit: Union[int, DefaultType] = DEFAULT,
        post_tags: Union[Sequence[str], DefaultType] = DEFAULT,
        pre_tags: Union[Sequence[str], DefaultType] = DEFAULT,
        require_field_match: Union[bool, DefaultType] = DEFAULT,
        tags_schema: Union[Literal["styled"], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if fields is not DEFAULT:
            kwargs["fields"] = str(fields)
        if encoder is not DEFAULT:
            kwargs["encoder"] = encoder
        if type is not DEFAULT:
            kwargs["type"] = type
        if boundary_chars is not DEFAULT:
            kwargs["boundary_chars"] = boundary_chars
        if boundary_max_scan is not DEFAULT:
            kwargs["boundary_max_scan"] = boundary_max_scan
        if boundary_scanner is not DEFAULT:
            kwargs["boundary_scanner"] = boundary_scanner
        if boundary_scanner_locale is not DEFAULT:
            kwargs["boundary_scanner_locale"] = boundary_scanner_locale
        if force_source is not DEFAULT:
            kwargs["force_source"] = force_source
        if fragmenter is not DEFAULT:
            kwargs["fragmenter"] = fragmenter
        if fragment_size is not DEFAULT:
            kwargs["fragment_size"] = fragment_size
        if highlight_filter is not DEFAULT:
            kwargs["highlight_filter"] = highlight_filter
        if highlight_query is not DEFAULT:
            kwargs["highlight_query"] = highlight_query
        if max_fragment_length is not DEFAULT:
            kwargs["max_fragment_length"] = max_fragment_length
        if max_analyzed_offset is not DEFAULT:
            kwargs["max_analyzed_offset"] = max_analyzed_offset
        if no_match_size is not DEFAULT:
            kwargs["no_match_size"] = no_match_size
        if number_of_fragments is not DEFAULT:
            kwargs["number_of_fragments"] = number_of_fragments
        if options is not DEFAULT:
            kwargs["options"] = options
        if order is not DEFAULT:
            kwargs["order"] = order
        if phrase_limit is not DEFAULT:
            kwargs["phrase_limit"] = phrase_limit
        if post_tags is not DEFAULT:
            kwargs["post_tags"] = post_tags
        if pre_tags is not DEFAULT:
            kwargs["pre_tags"] = pre_tags
        if require_field_match is not DEFAULT:
            kwargs["require_field_match"] = require_field_match
        if tags_schema is not DEFAULT:
            kwargs["tags_schema"] = tags_schema
        super().__init__(kwargs)


class HighlightField(AttrDict[Any]):
    """
    :arg fragment_offset:
    :arg matched_fields:
    :arg type:
    :arg boundary_chars: A string that contains each boundary character.
        Defaults to `.,!? \t\n` if omitted.
    :arg boundary_max_scan: How far to scan for boundary characters.
        Defaults to `20` if omitted.
    :arg boundary_scanner: Specifies how to break the highlighted
        fragments: chars, sentence, or word. Only valid for the unified
        and fvh highlighters. Defaults to `sentence` for the `unified`
        highlighter. Defaults to `chars` for the `fvh` highlighter.
    :arg boundary_scanner_locale: Controls which locale is used to search
        for sentence and word boundaries. This parameter takes a form of a
        language tag, for example: `"en-US"`, `"fr-FR"`, `"ja-JP"`.
        Defaults to `Locale.ROOT` if omitted.
    :arg force_source:
    :arg fragmenter: Specifies how text should be broken up in highlight
        snippets: `simple` or `span`. Only valid for the `plain`
        highlighter. Defaults to `span` if omitted.
    :arg fragment_size: The size of the highlighted fragment in
        characters. Defaults to `100` if omitted.
    :arg highlight_filter:
    :arg highlight_query: Highlight matches for a query other than the
        search query. This is especially useful if you use a rescore query
        because those are not taken into account by highlighting by
        default.
    :arg max_fragment_length:
    :arg max_analyzed_offset: If set to a non-negative value, highlighting
        stops at this defined maximum limit. The rest of the text is not
        processed, thus not highlighted and no error is returned The
        `max_analyzed_offset` query setting does not override the
        `index.highlight.max_analyzed_offset` setting, which prevails when
        it’s set to lower value than the query setting.
    :arg no_match_size: The amount of text you want to return from the
        beginning of the field if there are no matching fragments to
        highlight.
    :arg number_of_fragments: The maximum number of fragments to return.
        If the number of fragments is set to `0`, no fragments are
        returned. Instead, the entire field contents are highlighted and
        returned. This can be handy when you need to highlight short texts
        such as a title or address, but fragmentation is not required. If
        `number_of_fragments` is `0`, `fragment_size` is ignored. Defaults
        to `5` if omitted.
    :arg options:
    :arg order: Sorts highlighted fragments by score when set to `score`.
        By default, fragments will be output in the order they appear in
        the field (order: `none`). Setting this option to `score` will
        output the most relevant fragments first. Each highlighter applies
        its own logic to compute relevancy scores. Defaults to `none` if
        omitted.
    :arg phrase_limit: Controls the number of matching phrases in a
        document that are considered. Prevents the `fvh` highlighter from
        analyzing too many phrases and consuming too much memory. When
        using `matched_fields`, `phrase_limit` phrases per matched field
        are considered. Raising the limit increases query time and
        consumes more memory. Only supported by the `fvh` highlighter.
        Defaults to `256` if omitted.
    :arg post_tags: Use in conjunction with `pre_tags` to define the HTML
        tags to use for the highlighted text. By default, highlighted text
        is wrapped in `<em>` and `</em>` tags.
    :arg pre_tags: Use in conjunction with `post_tags` to define the HTML
        tags to use for the highlighted text. By default, highlighted text
        is wrapped in `<em>` and `</em>` tags.
    :arg require_field_match: By default, only fields that contains a
        query match are highlighted. Set to `false` to highlight all
        fields. Defaults to `True` if omitted.
    :arg tags_schema: Set to `styled` to use the built-in tag schema.
    """

    fragment_offset: Union[int, DefaultType]
    matched_fields: Union[
        Union[str, InstrumentedField],
        Sequence[Union[str, InstrumentedField]],
        DefaultType,
    ]
    type: Union[Literal["plain", "fvh", "unified"], DefaultType]
    boundary_chars: Union[str, DefaultType]
    boundary_max_scan: Union[int, DefaultType]
    boundary_scanner: Union[Literal["chars", "sentence", "word"], DefaultType]
    boundary_scanner_locale: Union[str, DefaultType]
    force_source: Union[bool, DefaultType]
    fragmenter: Union[Literal["simple", "span"], DefaultType]
    fragment_size: Union[int, DefaultType]
    highlight_filter: Union[bool, DefaultType]
    highlight_query: Union[Query, DefaultType]
    max_fragment_length: Union[int, DefaultType]
    max_analyzed_offset: Union[int, DefaultType]
    no_match_size: Union[int, DefaultType]
    number_of_fragments: Union[int, DefaultType]
    options: Union[Mapping[str, Any], DefaultType]
    order: Union[Literal["score"], DefaultType]
    phrase_limit: Union[int, DefaultType]
    post_tags: Union[Sequence[str], DefaultType]
    pre_tags: Union[Sequence[str], DefaultType]
    require_field_match: Union[bool, DefaultType]
    tags_schema: Union[Literal["styled"], DefaultType]

    def __init__(
        self,
        *,
        fragment_offset: Union[int, DefaultType] = DEFAULT,
        matched_fields: Union[
            Union[str, InstrumentedField],
            Sequence[Union[str, InstrumentedField]],
            DefaultType,
        ] = DEFAULT,
        type: Union[Literal["plain", "fvh", "unified"], DefaultType] = DEFAULT,
        boundary_chars: Union[str, DefaultType] = DEFAULT,
        boundary_max_scan: Union[int, DefaultType] = DEFAULT,
        boundary_scanner: Union[
            Literal["chars", "sentence", "word"], DefaultType
        ] = DEFAULT,
        boundary_scanner_locale: Union[str, DefaultType] = DEFAULT,
        force_source: Union[bool, DefaultType] = DEFAULT,
        fragmenter: Union[Literal["simple", "span"], DefaultType] = DEFAULT,
        fragment_size: Union[int, DefaultType] = DEFAULT,
        highlight_filter: Union[bool, DefaultType] = DEFAULT,
        highlight_query: Union[Query, DefaultType] = DEFAULT,
        max_fragment_length: Union[int, DefaultType] = DEFAULT,
        max_analyzed_offset: Union[int, DefaultType] = DEFAULT,
        no_match_size: Union[int, DefaultType] = DEFAULT,
        number_of_fragments: Union[int, DefaultType] = DEFAULT,
        options: Union[Mapping[str, Any], DefaultType] = DEFAULT,
        order: Union[Literal["score"], DefaultType] = DEFAULT,
        phrase_limit: Union[int, DefaultType] = DEFAULT,
        post_tags: Union[Sequence[str], DefaultType] = DEFAULT,
        pre_tags: Union[Sequence[str], DefaultType] = DEFAULT,
        require_field_match: Union[bool, DefaultType] = DEFAULT,
        tags_schema: Union[Literal["styled"], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if fragment_offset is not DEFAULT:
            kwargs["fragment_offset"] = fragment_offset
        if matched_fields is not DEFAULT:
            kwargs["matched_fields"] = str(matched_fields)
        if type is not DEFAULT:
            kwargs["type"] = type
        if boundary_chars is not DEFAULT:
            kwargs["boundary_chars"] = boundary_chars
        if boundary_max_scan is not DEFAULT:
            kwargs["boundary_max_scan"] = boundary_max_scan
        if boundary_scanner is not DEFAULT:
            kwargs["boundary_scanner"] = boundary_scanner
        if boundary_scanner_locale is not DEFAULT:
            kwargs["boundary_scanner_locale"] = boundary_scanner_locale
        if force_source is not DEFAULT:
            kwargs["force_source"] = force_source
        if fragmenter is not DEFAULT:
            kwargs["fragmenter"] = fragmenter
        if fragment_size is not DEFAULT:
            kwargs["fragment_size"] = fragment_size
        if highlight_filter is not DEFAULT:
            kwargs["highlight_filter"] = highlight_filter
        if highlight_query is not DEFAULT:
            kwargs["highlight_query"] = highlight_query
        if max_fragment_length is not DEFAULT:
            kwargs["max_fragment_length"] = max_fragment_length
        if max_analyzed_offset is not DEFAULT:
            kwargs["max_analyzed_offset"] = max_analyzed_offset
        if no_match_size is not DEFAULT:
            kwargs["no_match_size"] = no_match_size
        if number_of_fragments is not DEFAULT:
            kwargs["number_of_fragments"] = number_of_fragments
        if options is not DEFAULT:
            kwargs["options"] = options
        if order is not DEFAULT:
            kwargs["order"] = order
        if phrase_limit is not DEFAULT:
            kwargs["phrase_limit"] = phrase_limit
        if post_tags is not DEFAULT:
            kwargs["post_tags"] = post_tags
        if pre_tags is not DEFAULT:
            kwargs["pre_tags"] = pre_tags
        if require_field_match is not DEFAULT:
            kwargs["require_field_match"] = require_field_match
        if tags_schema is not DEFAULT:
            kwargs["tags_schema"] = tags_schema
        super().__init__(kwargs)


class HoltLinearModelSettings(AttrDict[Any]):
    """
    :arg alpha:
    :arg beta:
    """

    alpha: Union[float, DefaultType]
    beta: Union[float, DefaultType]

    def __init__(
        self,
        *,
        alpha: Union[float, DefaultType] = DEFAULT,
        beta: Union[float, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if alpha is not DEFAULT:
            kwargs["alpha"] = alpha
        if beta is not DEFAULT:
            kwargs["beta"] = beta
        super().__init__(kwargs)


class HoltWintersModelSettings(AttrDict[Any]):
    """
    :arg alpha:
    :arg beta:
    :arg gamma:
    :arg pad:
    :arg period:
    :arg type:
    """

    alpha: Union[float, DefaultType]
    beta: Union[float, DefaultType]
    gamma: Union[float, DefaultType]
    pad: Union[bool, DefaultType]
    period: Union[int, DefaultType]
    type: Union[Literal["add", "mult"], DefaultType]

    def __init__(
        self,
        *,
        alpha: Union[float, DefaultType] = DEFAULT,
        beta: Union[float, DefaultType] = DEFAULT,
        gamma: Union[float, DefaultType] = DEFAULT,
        pad: Union[bool, DefaultType] = DEFAULT,
        period: Union[int, DefaultType] = DEFAULT,
        type: Union[Literal["add", "mult"], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if alpha is not DEFAULT:
            kwargs["alpha"] = alpha
        if beta is not DEFAULT:
            kwargs["beta"] = beta
        if gamma is not DEFAULT:
            kwargs["gamma"] = gamma
        if pad is not DEFAULT:
            kwargs["pad"] = pad
        if period is not DEFAULT:
            kwargs["period"] = period
        if type is not DEFAULT:
            kwargs["type"] = type
        super().__init__(kwargs)


class InferenceConfigContainer(AttrDict[Any]):
    """
    :arg regression: Regression configuration for inference.
    :arg classification: Classification configuration for inference.
    """

    regression: Union["RegressionInferenceOptions", Dict[str, Any], DefaultType]
    classification: Union["ClassificationInferenceOptions", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        regression: Union[
            "RegressionInferenceOptions", Dict[str, Any], DefaultType
        ] = DEFAULT,
        classification: Union[
            "ClassificationInferenceOptions", Dict[str, Any], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if regression is not DEFAULT:
            kwargs["regression"] = regression
        if classification is not DEFAULT:
            kwargs["classification"] = classification
        super().__init__(kwargs)


class InnerHits(AttrDict[Any]):
    """
    :arg name: The name for the particular inner hit definition in the
        response. Useful when a search request contains multiple inner
        hits.
    :arg size: The maximum number of hits to return per `inner_hits`.
        Defaults to `3` if omitted.
    :arg from: Inner hit starting document offset.
    :arg collapse:
    :arg docvalue_fields:
    :arg explain:
    :arg highlight:
    :arg ignore_unmapped:
    :arg script_fields:
    :arg seq_no_primary_term:
    :arg fields:
    :arg sort: How the inner hits should be sorted per `inner_hits`. By
        default, inner hits are sorted by score.
    :arg _source:
    :arg stored_fields:
    :arg track_scores:
    :arg version:
    """

    name: Union[str, DefaultType]
    size: Union[int, DefaultType]
    from_: Union[int, DefaultType]
    collapse: Union["FieldCollapse", Dict[str, Any], DefaultType]
    docvalue_fields: Union[
        Sequence["FieldAndFormat"], Sequence[Dict[str, Any]], DefaultType
    ]
    explain: Union[bool, DefaultType]
    highlight: Union["Highlight", Dict[str, Any], DefaultType]
    ignore_unmapped: Union[bool, DefaultType]
    script_fields: Union[
        Mapping[Union[str, InstrumentedField], "ScriptField"],
        Dict[str, Any],
        DefaultType,
    ]
    seq_no_primary_term: Union[bool, DefaultType]
    fields: Union[Sequence[Union[str, InstrumentedField]], DefaultType]
    sort: Union[
        Union[Union[str, InstrumentedField], "SortOptions"],
        Sequence[Union[Union[str, InstrumentedField], "SortOptions"]],
        Dict[str, Any],
        DefaultType,
    ]
    _source: Union[bool, "SourceFilter", Dict[str, Any], DefaultType]
    stored_fields: Union[
        Union[str, InstrumentedField],
        Sequence[Union[str, InstrumentedField]],
        DefaultType,
    ]
    track_scores: Union[bool, DefaultType]
    version: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        name: Union[str, DefaultType] = DEFAULT,
        size: Union[int, DefaultType] = DEFAULT,
        from_: Union[int, DefaultType] = DEFAULT,
        collapse: Union["FieldCollapse", Dict[str, Any], DefaultType] = DEFAULT,
        docvalue_fields: Union[
            Sequence["FieldAndFormat"], Sequence[Dict[str, Any]], DefaultType
        ] = DEFAULT,
        explain: Union[bool, DefaultType] = DEFAULT,
        highlight: Union["Highlight", Dict[str, Any], DefaultType] = DEFAULT,
        ignore_unmapped: Union[bool, DefaultType] = DEFAULT,
        script_fields: Union[
            Mapping[Union[str, InstrumentedField], "ScriptField"],
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        seq_no_primary_term: Union[bool, DefaultType] = DEFAULT,
        fields: Union[Sequence[Union[str, InstrumentedField]], DefaultType] = DEFAULT,
        sort: Union[
            Union[Union[str, InstrumentedField], "SortOptions"],
            Sequence[Union[Union[str, InstrumentedField], "SortOptions"]],
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        _source: Union[bool, "SourceFilter", Dict[str, Any], DefaultType] = DEFAULT,
        stored_fields: Union[
            Union[str, InstrumentedField],
            Sequence[Union[str, InstrumentedField]],
            DefaultType,
        ] = DEFAULT,
        track_scores: Union[bool, DefaultType] = DEFAULT,
        version: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if name is not DEFAULT:
            kwargs["name"] = name
        if size is not DEFAULT:
            kwargs["size"] = size
        if from_ is not DEFAULT:
            kwargs["from_"] = from_
        if collapse is not DEFAULT:
            kwargs["collapse"] = collapse
        if docvalue_fields is not DEFAULT:
            kwargs["docvalue_fields"] = docvalue_fields
        if explain is not DEFAULT:
            kwargs["explain"] = explain
        if highlight is not DEFAULT:
            kwargs["highlight"] = highlight
        if ignore_unmapped is not DEFAULT:
            kwargs["ignore_unmapped"] = ignore_unmapped
        if script_fields is not DEFAULT:
            kwargs["script_fields"] = str(script_fields)
        if seq_no_primary_term is not DEFAULT:
            kwargs["seq_no_primary_term"] = seq_no_primary_term
        if fields is not DEFAULT:
            kwargs["fields"] = str(fields)
        if sort is not DEFAULT:
            kwargs["sort"] = str(sort)
        if _source is not DEFAULT:
            kwargs["_source"] = _source
        if stored_fields is not DEFAULT:
            kwargs["stored_fields"] = str(stored_fields)
        if track_scores is not DEFAULT:
            kwargs["track_scores"] = track_scores
        if version is not DEFAULT:
            kwargs["version"] = version
        super().__init__(kwargs)


class IntervalsAllOf(AttrDict[Any]):
    """
    :arg intervals: (required) An array of rules to combine. All rules
        must produce a match in a document for the overall source to
        match.
    :arg max_gaps: Maximum number of positions between the matching terms.
        Intervals produced by the rules further apart than this are not
        considered matches. Defaults to `-1` if omitted.
    :arg ordered: If `true`, intervals produced by the rules should appear
        in the order in which they are specified.
    :arg filter: Rule used to filter returned intervals.
    """

    intervals: Union[
        Sequence["IntervalsContainer"], Sequence[Dict[str, Any]], DefaultType
    ]
    max_gaps: Union[int, DefaultType]
    ordered: Union[bool, DefaultType]
    filter: Union["IntervalsFilter", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        intervals: Union[
            Sequence["IntervalsContainer"], Sequence[Dict[str, Any]], DefaultType
        ] = DEFAULT,
        max_gaps: Union[int, DefaultType] = DEFAULT,
        ordered: Union[bool, DefaultType] = DEFAULT,
        filter: Union["IntervalsFilter", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if intervals is not DEFAULT:
            kwargs["intervals"] = intervals
        if max_gaps is not DEFAULT:
            kwargs["max_gaps"] = max_gaps
        if ordered is not DEFAULT:
            kwargs["ordered"] = ordered
        if filter is not DEFAULT:
            kwargs["filter"] = filter
        super().__init__(kwargs)


class IntervalsAnyOf(AttrDict[Any]):
    """
    :arg intervals: (required) An array of rules to match.
    :arg filter: Rule used to filter returned intervals.
    """

    intervals: Union[
        Sequence["IntervalsContainer"], Sequence[Dict[str, Any]], DefaultType
    ]
    filter: Union["IntervalsFilter", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        intervals: Union[
            Sequence["IntervalsContainer"], Sequence[Dict[str, Any]], DefaultType
        ] = DEFAULT,
        filter: Union["IntervalsFilter", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if intervals is not DEFAULT:
            kwargs["intervals"] = intervals
        if filter is not DEFAULT:
            kwargs["filter"] = filter
        super().__init__(kwargs)


class IntervalsContainer(AttrDict[Any]):
    """
    :arg all_of: Returns matches that span a combination of other rules.
    :arg any_of: Returns intervals produced by any of its sub-rules.
    :arg fuzzy: Matches analyzed text.
    :arg match: Matches analyzed text.
    :arg prefix: Matches terms that start with a specified set of
        characters.
    :arg range:
    :arg regexp:
    :arg wildcard: Matches terms using a wildcard pattern.
    """

    all_of: Union["IntervalsAllOf", Dict[str, Any], DefaultType]
    any_of: Union["IntervalsAnyOf", Dict[str, Any], DefaultType]
    fuzzy: Union["IntervalsFuzzy", Dict[str, Any], DefaultType]
    match: Union["IntervalsMatch", Dict[str, Any], DefaultType]
    prefix: Union["IntervalsPrefix", Dict[str, Any], DefaultType]
    range: Union["IntervalsRange", Dict[str, Any], DefaultType]
    regexp: Union["IntervalsRegexp", Dict[str, Any], DefaultType]
    wildcard: Union["IntervalsWildcard", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        all_of: Union["IntervalsAllOf", Dict[str, Any], DefaultType] = DEFAULT,
        any_of: Union["IntervalsAnyOf", Dict[str, Any], DefaultType] = DEFAULT,
        fuzzy: Union["IntervalsFuzzy", Dict[str, Any], DefaultType] = DEFAULT,
        match: Union["IntervalsMatch", Dict[str, Any], DefaultType] = DEFAULT,
        prefix: Union["IntervalsPrefix", Dict[str, Any], DefaultType] = DEFAULT,
        range: Union["IntervalsRange", Dict[str, Any], DefaultType] = DEFAULT,
        regexp: Union["IntervalsRegexp", Dict[str, Any], DefaultType] = DEFAULT,
        wildcard: Union["IntervalsWildcard", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if all_of is not DEFAULT:
            kwargs["all_of"] = all_of
        if any_of is not DEFAULT:
            kwargs["any_of"] = any_of
        if fuzzy is not DEFAULT:
            kwargs["fuzzy"] = fuzzy
        if match is not DEFAULT:
            kwargs["match"] = match
        if prefix is not DEFAULT:
            kwargs["prefix"] = prefix
        if range is not DEFAULT:
            kwargs["range"] = range
        if regexp is not DEFAULT:
            kwargs["regexp"] = regexp
        if wildcard is not DEFAULT:
            kwargs["wildcard"] = wildcard
        super().__init__(kwargs)


class IntervalsFilter(AttrDict[Any]):
    """
    :arg after: Query used to return intervals that follow an interval
        from the `filter` rule.
    :arg before: Query used to return intervals that occur before an
        interval from the `filter` rule.
    :arg contained_by: Query used to return intervals contained by an
        interval from the `filter` rule.
    :arg containing: Query used to return intervals that contain an
        interval from the `filter` rule.
    :arg not_contained_by: Query used to return intervals that are **not**
        contained by an interval from the `filter` rule.
    :arg not_containing: Query used to return intervals that do **not**
        contain an interval from the `filter` rule.
    :arg not_overlapping: Query used to return intervals that do **not**
        overlap with an interval from the `filter` rule.
    :arg overlapping: Query used to return intervals that overlap with an
        interval from the `filter` rule.
    :arg script: Script used to return matching documents. This script
        must return a boolean value: `true` or `false`.
    """

    after: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    before: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    contained_by: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    containing: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    not_contained_by: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    not_containing: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    not_overlapping: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    overlapping: Union["IntervalsContainer", Dict[str, Any], DefaultType]
    script: Union["Script", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        after: Union["IntervalsContainer", Dict[str, Any], DefaultType] = DEFAULT,
        before: Union["IntervalsContainer", Dict[str, Any], DefaultType] = DEFAULT,
        contained_by: Union[
            "IntervalsContainer", Dict[str, Any], DefaultType
        ] = DEFAULT,
        containing: Union["IntervalsContainer", Dict[str, Any], DefaultType] = DEFAULT,
        not_contained_by: Union[
            "IntervalsContainer", Dict[str, Any], DefaultType
        ] = DEFAULT,
        not_containing: Union[
            "IntervalsContainer", Dict[str, Any], DefaultType
        ] = DEFAULT,
        not_overlapping: Union[
            "IntervalsContainer", Dict[str, Any], DefaultType
        ] = DEFAULT,
        overlapping: Union["IntervalsContainer", Dict[str, Any], DefaultType] = DEFAULT,
        script: Union["Script", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if after is not DEFAULT:
            kwargs["after"] = after
        if before is not DEFAULT:
            kwargs["before"] = before
        if contained_by is not DEFAULT:
            kwargs["contained_by"] = contained_by
        if containing is not DEFAULT:
            kwargs["containing"] = containing
        if not_contained_by is not DEFAULT:
            kwargs["not_contained_by"] = not_contained_by
        if not_containing is not DEFAULT:
            kwargs["not_containing"] = not_containing
        if not_overlapping is not DEFAULT:
            kwargs["not_overlapping"] = not_overlapping
        if overlapping is not DEFAULT:
            kwargs["overlapping"] = overlapping
        if script is not DEFAULT:
            kwargs["script"] = script
        super().__init__(kwargs)


class IntervalsFuzzy(AttrDict[Any]):
    """
    :arg term: (required) The term to match.
    :arg analyzer: Analyzer used to normalize the term.
    :arg fuzziness: Maximum edit distance allowed for matching. Defaults
        to `auto` if omitted.
    :arg prefix_length: Number of beginning characters left unchanged when
        creating expansions.
    :arg transpositions: Indicates whether edits include transpositions of
        two adjacent characters (for example, `ab` to `ba`). Defaults to
        `True` if omitted.
    :arg use_field: If specified, match intervals from this field rather
        than the top-level field. The `term` is normalized using the
        search analyzer from this field, unless `analyzer` is specified
        separately.
    """

    term: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    fuzziness: Union[str, int, DefaultType]
    prefix_length: Union[int, DefaultType]
    transpositions: Union[bool, DefaultType]
    use_field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        term: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        fuzziness: Union[str, int, DefaultType] = DEFAULT,
        prefix_length: Union[int, DefaultType] = DEFAULT,
        transpositions: Union[bool, DefaultType] = DEFAULT,
        use_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if term is not DEFAULT:
            kwargs["term"] = term
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if fuzziness is not DEFAULT:
            kwargs["fuzziness"] = fuzziness
        if prefix_length is not DEFAULT:
            kwargs["prefix_length"] = prefix_length
        if transpositions is not DEFAULT:
            kwargs["transpositions"] = transpositions
        if use_field is not DEFAULT:
            kwargs["use_field"] = str(use_field)
        super().__init__(kwargs)


class IntervalsMatch(AttrDict[Any]):
    """
    :arg query: (required) Text you wish to find in the provided field.
    :arg analyzer: Analyzer used to analyze terms in the query.
    :arg max_gaps: Maximum number of positions between the matching terms.
        Terms further apart than this are not considered matches. Defaults
        to `-1` if omitted.
    :arg ordered: If `true`, matching terms must appear in their specified
        order.
    :arg use_field: If specified, match intervals from this field rather
        than the top-level field. The `term` is normalized using the
        search analyzer from this field, unless `analyzer` is specified
        separately.
    :arg filter: An optional interval filter.
    """

    query: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    max_gaps: Union[int, DefaultType]
    ordered: Union[bool, DefaultType]
    use_field: Union[str, InstrumentedField, DefaultType]
    filter: Union["IntervalsFilter", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        query: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        max_gaps: Union[int, DefaultType] = DEFAULT,
        ordered: Union[bool, DefaultType] = DEFAULT,
        use_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        filter: Union["IntervalsFilter", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if query is not DEFAULT:
            kwargs["query"] = query
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if max_gaps is not DEFAULT:
            kwargs["max_gaps"] = max_gaps
        if ordered is not DEFAULT:
            kwargs["ordered"] = ordered
        if use_field is not DEFAULT:
            kwargs["use_field"] = str(use_field)
        if filter is not DEFAULT:
            kwargs["filter"] = filter
        super().__init__(kwargs)


class IntervalsPrefix(AttrDict[Any]):
    """
    :arg prefix: (required) Beginning characters of terms you wish to find
        in the top-level field.
    :arg analyzer: Analyzer used to analyze the `prefix`.
    :arg use_field: If specified, match intervals from this field rather
        than the top-level field. The `prefix` is normalized using the
        search analyzer from this field, unless `analyzer` is specified
        separately.
    """

    prefix: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    use_field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        prefix: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        use_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if prefix is not DEFAULT:
            kwargs["prefix"] = prefix
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if use_field is not DEFAULT:
            kwargs["use_field"] = str(use_field)
        super().__init__(kwargs)


class IntervalsQuery(AttrDict[Any]):
    """
    :arg all_of: Returns matches that span a combination of other rules.
    :arg any_of: Returns intervals produced by any of its sub-rules.
    :arg fuzzy: Matches terms that are similar to the provided term,
        within an edit distance defined by `fuzziness`.
    :arg match: Matches analyzed text.
    :arg prefix: Matches terms that start with a specified set of
        characters.
    :arg range:
    :arg regexp:
    :arg wildcard: Matches terms using a wildcard pattern.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    all_of: Union["IntervalsAllOf", Dict[str, Any], DefaultType]
    any_of: Union["IntervalsAnyOf", Dict[str, Any], DefaultType]
    fuzzy: Union["IntervalsFuzzy", Dict[str, Any], DefaultType]
    match: Union["IntervalsMatch", Dict[str, Any], DefaultType]
    prefix: Union["IntervalsPrefix", Dict[str, Any], DefaultType]
    range: Union["IntervalsRange", Dict[str, Any], DefaultType]
    regexp: Union["IntervalsRegexp", Dict[str, Any], DefaultType]
    wildcard: Union["IntervalsWildcard", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        all_of: Union["IntervalsAllOf", Dict[str, Any], DefaultType] = DEFAULT,
        any_of: Union["IntervalsAnyOf", Dict[str, Any], DefaultType] = DEFAULT,
        fuzzy: Union["IntervalsFuzzy", Dict[str, Any], DefaultType] = DEFAULT,
        match: Union["IntervalsMatch", Dict[str, Any], DefaultType] = DEFAULT,
        prefix: Union["IntervalsPrefix", Dict[str, Any], DefaultType] = DEFAULT,
        range: Union["IntervalsRange", Dict[str, Any], DefaultType] = DEFAULT,
        regexp: Union["IntervalsRegexp", Dict[str, Any], DefaultType] = DEFAULT,
        wildcard: Union["IntervalsWildcard", Dict[str, Any], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if all_of is not DEFAULT:
            kwargs["all_of"] = all_of
        if any_of is not DEFAULT:
            kwargs["any_of"] = any_of
        if fuzzy is not DEFAULT:
            kwargs["fuzzy"] = fuzzy
        if match is not DEFAULT:
            kwargs["match"] = match
        if prefix is not DEFAULT:
            kwargs["prefix"] = prefix
        if range is not DEFAULT:
            kwargs["range"] = range
        if regexp is not DEFAULT:
            kwargs["regexp"] = regexp
        if wildcard is not DEFAULT:
            kwargs["wildcard"] = wildcard
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class IntervalsRange(AttrDict[Any]):
    """
    :arg analyzer: Analyzer used to analyze the `prefix`.
    :arg gte: Lower term, either gte or gt must be provided.
    :arg gt: Lower term, either gte or gt must be provided.
    :arg lte: Upper term, either lte or lt must be provided.
    :arg lt: Upper term, either lte or lt must be provided.
    :arg use_field: If specified, match intervals from this field rather
        than the top-level field. The `prefix` is normalized using the
        search analyzer from this field, unless `analyzer` is specified
        separately.
    """

    analyzer: Union[str, DefaultType]
    gte: Union[str, DefaultType]
    gt: Union[str, DefaultType]
    lte: Union[str, DefaultType]
    lt: Union[str, DefaultType]
    use_field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        analyzer: Union[str, DefaultType] = DEFAULT,
        gte: Union[str, DefaultType] = DEFAULT,
        gt: Union[str, DefaultType] = DEFAULT,
        lte: Union[str, DefaultType] = DEFAULT,
        lt: Union[str, DefaultType] = DEFAULT,
        use_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if gte is not DEFAULT:
            kwargs["gte"] = gte
        if gt is not DEFAULT:
            kwargs["gt"] = gt
        if lte is not DEFAULT:
            kwargs["lte"] = lte
        if lt is not DEFAULT:
            kwargs["lt"] = lt
        if use_field is not DEFAULT:
            kwargs["use_field"] = str(use_field)
        super().__init__(kwargs)


class IntervalsRegexp(AttrDict[Any]):
    """
    :arg pattern: (required) Regex pattern.
    :arg analyzer: Analyzer used to analyze the `prefix`.
    :arg use_field: If specified, match intervals from this field rather
        than the top-level field. The `prefix` is normalized using the
        search analyzer from this field, unless `analyzer` is specified
        separately.
    """

    pattern: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    use_field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        pattern: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        use_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if pattern is not DEFAULT:
            kwargs["pattern"] = pattern
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if use_field is not DEFAULT:
            kwargs["use_field"] = str(use_field)
        super().__init__(kwargs)


class IntervalsWildcard(AttrDict[Any]):
    """
    :arg pattern: (required) Wildcard pattern used to find matching terms.
    :arg analyzer: Analyzer used to analyze the `pattern`. Defaults to the
        top-level field's analyzer.
    :arg use_field: If specified, match intervals from this field rather
        than the top-level field. The `pattern` is normalized using the
        search analyzer from this field, unless `analyzer` is specified
        separately.
    """

    pattern: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    use_field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        pattern: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        use_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if pattern is not DEFAULT:
            kwargs["pattern"] = pattern
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if use_field is not DEFAULT:
            kwargs["use_field"] = str(use_field)
        super().__init__(kwargs)


class IpRangeAggregationRange(AttrDict[Any]):
    """
    :arg from: Start of the range.
    :arg mask: IP range defined as a CIDR mask.
    :arg to: End of the range.
    """

    from_: Union[str, None, DefaultType]
    mask: Union[str, DefaultType]
    to: Union[str, None, DefaultType]

    def __init__(
        self,
        *,
        from_: Union[str, None, DefaultType] = DEFAULT,
        mask: Union[str, DefaultType] = DEFAULT,
        to: Union[str, None, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if from_ is not DEFAULT:
            kwargs["from_"] = from_
        if mask is not DEFAULT:
            kwargs["mask"] = mask
        if to is not DEFAULT:
            kwargs["to"] = to
        super().__init__(kwargs)


class LatLonGeoLocation(AttrDict[Any]):
    """
    :arg lat: (required) Latitude
    :arg lon: (required) Longitude
    """

    lat: Union[float, DefaultType]
    lon: Union[float, DefaultType]

    def __init__(
        self,
        *,
        lat: Union[float, DefaultType] = DEFAULT,
        lon: Union[float, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if lat is not DEFAULT:
            kwargs["lat"] = lat
        if lon is not DEFAULT:
            kwargs["lon"] = lon
        super().__init__(kwargs)


class LikeDocument(AttrDict[Any]):
    """
    :arg doc: A document not present in the index.
    :arg fields:
    :arg _id: ID of a document.
    :arg _index: Index of a document.
    :arg per_field_analyzer: Overrides the default analyzer.
    :arg routing:
    :arg version:
    :arg version_type:  Defaults to `'internal'` if omitted.
    """

    doc: Any
    fields: Union[Sequence[Union[str, InstrumentedField]], DefaultType]
    _id: Union[str, DefaultType]
    _index: Union[str, DefaultType]
    per_field_analyzer: Union[Mapping[Union[str, InstrumentedField], str], DefaultType]
    routing: Union[str, DefaultType]
    version: Union[int, DefaultType]
    version_type: Union[
        Literal["internal", "external", "external_gte", "force"], DefaultType
    ]

    def __init__(
        self,
        *,
        doc: Any = DEFAULT,
        fields: Union[Sequence[Union[str, InstrumentedField]], DefaultType] = DEFAULT,
        _id: Union[str, DefaultType] = DEFAULT,
        _index: Union[str, DefaultType] = DEFAULT,
        per_field_analyzer: Union[
            Mapping[Union[str, InstrumentedField], str], DefaultType
        ] = DEFAULT,
        routing: Union[str, DefaultType] = DEFAULT,
        version: Union[int, DefaultType] = DEFAULT,
        version_type: Union[
            Literal["internal", "external", "external_gte", "force"], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if doc is not DEFAULT:
            kwargs["doc"] = doc
        if fields is not DEFAULT:
            kwargs["fields"] = str(fields)
        if _id is not DEFAULT:
            kwargs["_id"] = _id
        if _index is not DEFAULT:
            kwargs["_index"] = _index
        if per_field_analyzer is not DEFAULT:
            kwargs["per_field_analyzer"] = str(per_field_analyzer)
        if routing is not DEFAULT:
            kwargs["routing"] = routing
        if version is not DEFAULT:
            kwargs["version"] = version
        if version_type is not DEFAULT:
            kwargs["version_type"] = version_type
        super().__init__(kwargs)


class MatchBoolPrefixQuery(AttrDict[Any]):
    """
    :arg query: (required) Terms you wish to find in the provided field.
        The last term is used in a prefix query.
    :arg analyzer: Analyzer used to convert the text in the query value
        into tokens.
    :arg fuzziness: Maximum edit distance allowed for matching. Can be
        applied to the term subqueries constructed for all terms but the
        final term.
    :arg fuzzy_rewrite: Method used to rewrite the query. Can be applied
        to the term subqueries constructed for all terms but the final
        term.
    :arg fuzzy_transpositions: If `true`, edits for fuzzy matching include
        transpositions of two adjacent characters (for example, `ab` to
        `ba`). Can be applied to the term subqueries constructed for all
        terms but the final term. Defaults to `True` if omitted.
    :arg max_expansions: Maximum number of terms to which the query will
        expand. Can be applied to the term subqueries constructed for all
        terms but the final term. Defaults to `50` if omitted.
    :arg minimum_should_match: Minimum number of clauses that must match
        for a document to be returned. Applied to the constructed bool
        query.
    :arg operator: Boolean logic used to interpret text in the query
        value. Applied to the constructed bool query. Defaults to `'or'`
        if omitted.
    :arg prefix_length: Number of beginning characters left unchanged for
        fuzzy matching. Can be applied to the term subqueries constructed
        for all terms but the final term.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    query: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    fuzziness: Union[str, int, DefaultType]
    fuzzy_rewrite: Union[str, DefaultType]
    fuzzy_transpositions: Union[bool, DefaultType]
    max_expansions: Union[int, DefaultType]
    minimum_should_match: Union[int, str, DefaultType]
    operator: Union[Literal["and", "or"], DefaultType]
    prefix_length: Union[int, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        query: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        fuzziness: Union[str, int, DefaultType] = DEFAULT,
        fuzzy_rewrite: Union[str, DefaultType] = DEFAULT,
        fuzzy_transpositions: Union[bool, DefaultType] = DEFAULT,
        max_expansions: Union[int, DefaultType] = DEFAULT,
        minimum_should_match: Union[int, str, DefaultType] = DEFAULT,
        operator: Union[Literal["and", "or"], DefaultType] = DEFAULT,
        prefix_length: Union[int, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if query is not DEFAULT:
            kwargs["query"] = query
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if fuzziness is not DEFAULT:
            kwargs["fuzziness"] = fuzziness
        if fuzzy_rewrite is not DEFAULT:
            kwargs["fuzzy_rewrite"] = fuzzy_rewrite
        if fuzzy_transpositions is not DEFAULT:
            kwargs["fuzzy_transpositions"] = fuzzy_transpositions
        if max_expansions is not DEFAULT:
            kwargs["max_expansions"] = max_expansions
        if minimum_should_match is not DEFAULT:
            kwargs["minimum_should_match"] = minimum_should_match
        if operator is not DEFAULT:
            kwargs["operator"] = operator
        if prefix_length is not DEFAULT:
            kwargs["prefix_length"] = prefix_length
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class MatchPhrasePrefixQuery(AttrDict[Any]):
    """
    :arg query: (required) Text you wish to find in the provided field.
    :arg analyzer: Analyzer used to convert text in the query value into
        tokens.
    :arg max_expansions: Maximum number of terms to which the last
        provided term of the query value will expand. Defaults to `50` if
        omitted.
    :arg slop: Maximum number of positions allowed between matching
        tokens.
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

    query: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    max_expansions: Union[int, DefaultType]
    slop: Union[int, DefaultType]
    zero_terms_query: Union[Literal["all", "none"], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        query: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        max_expansions: Union[int, DefaultType] = DEFAULT,
        slop: Union[int, DefaultType] = DEFAULT,
        zero_terms_query: Union[Literal["all", "none"], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if query is not DEFAULT:
            kwargs["query"] = query
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if max_expansions is not DEFAULT:
            kwargs["max_expansions"] = max_expansions
        if slop is not DEFAULT:
            kwargs["slop"] = slop
        if zero_terms_query is not DEFAULT:
            kwargs["zero_terms_query"] = zero_terms_query
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class MatchPhraseQuery(AttrDict[Any]):
    """
    :arg query: (required) Query terms that are analyzed and turned into a
        phrase query.
    :arg analyzer: Analyzer used to convert the text in the query value
        into tokens.
    :arg slop: Maximum number of positions allowed between matching
        tokens.
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

    query: Union[str, DefaultType]
    analyzer: Union[str, DefaultType]
    slop: Union[int, DefaultType]
    zero_terms_query: Union[Literal["all", "none"], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        query: Union[str, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        slop: Union[int, DefaultType] = DEFAULT,
        zero_terms_query: Union[Literal["all", "none"], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if query is not DEFAULT:
            kwargs["query"] = query
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if slop is not DEFAULT:
            kwargs["slop"] = slop
        if zero_terms_query is not DEFAULT:
            kwargs["zero_terms_query"] = zero_terms_query
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class MatchQuery(AttrDict[Any]):
    """
    :arg query: (required) Text, number, boolean value or date you wish to
        find in the provided field.
    :arg analyzer: Analyzer used to convert the text in the query value
        into tokens.
    :arg auto_generate_synonyms_phrase_query: If `true`, match phrase
        queries are automatically created for multi-term synonyms.
        Defaults to `True` if omitted.
    :arg cutoff_frequency:
    :arg fuzziness: Maximum edit distance allowed for matching.
    :arg fuzzy_rewrite: Method used to rewrite the query.
    :arg fuzzy_transpositions: If `true`, edits for fuzzy matching include
        transpositions of two adjacent characters (for example, `ab` to
        `ba`). Defaults to `True` if omitted.
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

    query: Union[str, float, bool, DefaultType]
    analyzer: Union[str, DefaultType]
    auto_generate_synonyms_phrase_query: Union[bool, DefaultType]
    cutoff_frequency: Union[float, DefaultType]
    fuzziness: Union[str, int, DefaultType]
    fuzzy_rewrite: Union[str, DefaultType]
    fuzzy_transpositions: Union[bool, DefaultType]
    lenient: Union[bool, DefaultType]
    max_expansions: Union[int, DefaultType]
    minimum_should_match: Union[int, str, DefaultType]
    operator: Union[Literal["and", "or"], DefaultType]
    prefix_length: Union[int, DefaultType]
    zero_terms_query: Union[Literal["all", "none"], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        query: Union[str, float, bool, DefaultType] = DEFAULT,
        analyzer: Union[str, DefaultType] = DEFAULT,
        auto_generate_synonyms_phrase_query: Union[bool, DefaultType] = DEFAULT,
        cutoff_frequency: Union[float, DefaultType] = DEFAULT,
        fuzziness: Union[str, int, DefaultType] = DEFAULT,
        fuzzy_rewrite: Union[str, DefaultType] = DEFAULT,
        fuzzy_transpositions: Union[bool, DefaultType] = DEFAULT,
        lenient: Union[bool, DefaultType] = DEFAULT,
        max_expansions: Union[int, DefaultType] = DEFAULT,
        minimum_should_match: Union[int, str, DefaultType] = DEFAULT,
        operator: Union[Literal["and", "or"], DefaultType] = DEFAULT,
        prefix_length: Union[int, DefaultType] = DEFAULT,
        zero_terms_query: Union[Literal["all", "none"], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if query is not DEFAULT:
            kwargs["query"] = query
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if auto_generate_synonyms_phrase_query is not DEFAULT:
            kwargs["auto_generate_synonyms_phrase_query"] = (
                auto_generate_synonyms_phrase_query
            )
        if cutoff_frequency is not DEFAULT:
            kwargs["cutoff_frequency"] = cutoff_frequency
        if fuzziness is not DEFAULT:
            kwargs["fuzziness"] = fuzziness
        if fuzzy_rewrite is not DEFAULT:
            kwargs["fuzzy_rewrite"] = fuzzy_rewrite
        if fuzzy_transpositions is not DEFAULT:
            kwargs["fuzzy_transpositions"] = fuzzy_transpositions
        if lenient is not DEFAULT:
            kwargs["lenient"] = lenient
        if max_expansions is not DEFAULT:
            kwargs["max_expansions"] = max_expansions
        if minimum_should_match is not DEFAULT:
            kwargs["minimum_should_match"] = minimum_should_match
        if operator is not DEFAULT:
            kwargs["operator"] = operator
        if prefix_length is not DEFAULT:
            kwargs["prefix_length"] = prefix_length
        if zero_terms_query is not DEFAULT:
            kwargs["zero_terms_query"] = zero_terms_query
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class MultiTermLookup(AttrDict[Any]):
    """
    :arg field: (required) A fields from which to retrieve terms.
    :arg missing: The value to apply to documents that do not have a
        value. By default, documents without a value are ignored.
    """

    field: Union[str, InstrumentedField, DefaultType]
    missing: Union[str, int, float, bool, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        missing: Union[str, int, float, bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if missing is not DEFAULT:
            kwargs["missing"] = missing
        super().__init__(kwargs)


class MutualInformationHeuristic(AttrDict[Any]):
    """
    :arg background_is_superset: Set to `false` if you defined a custom
        background filter that represents a different set of documents
        that you want to compare to.
    :arg include_negatives: Set to `false` to filter out the terms that
        appear less often in the subset than in documents outside the
        subset.
    """

    background_is_superset: Union[bool, DefaultType]
    include_negatives: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        background_is_superset: Union[bool, DefaultType] = DEFAULT,
        include_negatives: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if background_is_superset is not DEFAULT:
            kwargs["background_is_superset"] = background_is_superset
        if include_negatives is not DEFAULT:
            kwargs["include_negatives"] = include_negatives
        super().__init__(kwargs)


class NestedSortValue(AttrDict[Any]):
    """
    :arg path: (required)
    :arg filter:
    :arg max_children:
    :arg nested:
    """

    path: Union[str, InstrumentedField, DefaultType]
    filter: Union[Query, DefaultType]
    max_children: Union[int, DefaultType]
    nested: Union["NestedSortValue", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        path: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        filter: Union[Query, DefaultType] = DEFAULT,
        max_children: Union[int, DefaultType] = DEFAULT,
        nested: Union["NestedSortValue", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if path is not DEFAULT:
            kwargs["path"] = str(path)
        if filter is not DEFAULT:
            kwargs["filter"] = filter
        if max_children is not DEFAULT:
            kwargs["max_children"] = max_children
        if nested is not DEFAULT:
            kwargs["nested"] = nested
        super().__init__(kwargs)


class NumericFielddata(AttrDict[Any]):
    """
    :arg format: (required)
    """

    format: Union[Literal["array", "disabled"], DefaultType]

    def __init__(
        self,
        *,
        format: Union[Literal["array", "disabled"], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if format is not DEFAULT:
            kwargs["format"] = format
        super().__init__(kwargs)


class PercentageScoreHeuristic(AttrDict[Any]):
    pass


class PinnedDoc(AttrDict[Any]):
    """
    :arg _id: (required) The unique document ID.
    :arg _index: The index that contains the document.
    """

    _id: Union[str, DefaultType]
    _index: Union[str, DefaultType]

    def __init__(
        self,
        *,
        _id: Union[str, DefaultType] = DEFAULT,
        _index: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if _id is not DEFAULT:
            kwargs["_id"] = _id
        if _index is not DEFAULT:
            kwargs["_index"] = _index
        super().__init__(kwargs)


class PrefixQuery(AttrDict[Any]):
    """
    :arg value: (required) Beginning characters of terms you wish to find
        in the provided field.
    :arg rewrite: Method used to rewrite the query.
    :arg case_insensitive: Allows ASCII case insensitive matching of the
        value with the indexed field values when set to `true`. Default is
        `false` which means the case sensitivity of matching depends on
        the underlying field’s mapping.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    value: Union[str, DefaultType]
    rewrite: Union[str, DefaultType]
    case_insensitive: Union[bool, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        value: Union[str, DefaultType] = DEFAULT,
        rewrite: Union[str, DefaultType] = DEFAULT,
        case_insensitive: Union[bool, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if value is not DEFAULT:
            kwargs["value"] = value
        if rewrite is not DEFAULT:
            kwargs["rewrite"] = rewrite
        if case_insensitive is not DEFAULT:
            kwargs["case_insensitive"] = case_insensitive
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class QueryVectorBuilder(AttrDict[Any]):
    """
    :arg text_embedding:
    """

    text_embedding: Union["TextEmbedding", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        text_embedding: Union["TextEmbedding", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if text_embedding is not DEFAULT:
            kwargs["text_embedding"] = text_embedding
        super().__init__(kwargs)


class RankFeatureFunctionLinear(AttrDict[Any]):
    pass


class RankFeatureFunctionLogarithm(AttrDict[Any]):
    """
    :arg scaling_factor: (required) Configurable scaling factor.
    """

    scaling_factor: Union[float, DefaultType]

    def __init__(
        self, *, scaling_factor: Union[float, DefaultType] = DEFAULT, **kwargs: Any
    ):
        if scaling_factor is not DEFAULT:
            kwargs["scaling_factor"] = scaling_factor
        super().__init__(kwargs)


class RankFeatureFunctionSaturation(AttrDict[Any]):
    """
    :arg pivot: Configurable pivot value so that the result will be less
        than 0.5.
    """

    pivot: Union[float, DefaultType]

    def __init__(self, *, pivot: Union[float, DefaultType] = DEFAULT, **kwargs: Any):
        if pivot is not DEFAULT:
            kwargs["pivot"] = pivot
        super().__init__(kwargs)


class RankFeatureFunctionSigmoid(AttrDict[Any]):
    """
    :arg pivot: (required) Configurable pivot value so that the result
        will be less than 0.5.
    :arg exponent: (required) Configurable Exponent.
    """

    pivot: Union[float, DefaultType]
    exponent: Union[float, DefaultType]

    def __init__(
        self,
        *,
        pivot: Union[float, DefaultType] = DEFAULT,
        exponent: Union[float, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if pivot is not DEFAULT:
            kwargs["pivot"] = pivot
        if exponent is not DEFAULT:
            kwargs["exponent"] = exponent
        super().__init__(kwargs)


class RegexpQuery(AttrDict[Any]):
    """
    :arg value: (required) Regular expression for terms you wish to find
        in the provided field.
    :arg case_insensitive: Allows case insensitive matching of the regular
        expression value with the indexed field values when set to `true`.
        When `false`, case sensitivity of matching depends on the
        underlying field’s mapping.
    :arg flags: Enables optional operators for the regular expression.
    :arg max_determinized_states: Maximum number of automaton states
        required for the query. Defaults to `10000` if omitted.
    :arg rewrite: Method used to rewrite the query.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    value: Union[str, DefaultType]
    case_insensitive: Union[bool, DefaultType]
    flags: Union[str, DefaultType]
    max_determinized_states: Union[int, DefaultType]
    rewrite: Union[str, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        value: Union[str, DefaultType] = DEFAULT,
        case_insensitive: Union[bool, DefaultType] = DEFAULT,
        flags: Union[str, DefaultType] = DEFAULT,
        max_determinized_states: Union[int, DefaultType] = DEFAULT,
        rewrite: Union[str, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if value is not DEFAULT:
            kwargs["value"] = value
        if case_insensitive is not DEFAULT:
            kwargs["case_insensitive"] = case_insensitive
        if flags is not DEFAULT:
            kwargs["flags"] = flags
        if max_determinized_states is not DEFAULT:
            kwargs["max_determinized_states"] = max_determinized_states
        if rewrite is not DEFAULT:
            kwargs["rewrite"] = rewrite
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class RegressionInferenceOptions(AttrDict[Any]):
    """
    :arg results_field: The field that is added to incoming documents to
        contain the inference prediction. Defaults to predicted_value.
    :arg num_top_feature_importance_values: Specifies the maximum number
        of feature importance values per document.
    """

    results_field: Union[str, InstrumentedField, DefaultType]
    num_top_feature_importance_values: Union[int, DefaultType]

    def __init__(
        self,
        *,
        results_field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        num_top_feature_importance_values: Union[int, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if results_field is not DEFAULT:
            kwargs["results_field"] = str(results_field)
        if num_top_feature_importance_values is not DEFAULT:
            kwargs["num_top_feature_importance_values"] = (
                num_top_feature_importance_values
            )
        super().__init__(kwargs)


class RescoreVector(AttrDict[Any]):
    """
    :arg oversample: (required) Applies the specified oversample factor to
        k on the approximate kNN search
    """

    oversample: Union[float, DefaultType]

    def __init__(
        self, *, oversample: Union[float, DefaultType] = DEFAULT, **kwargs: Any
    ):
        if oversample is not DEFAULT:
            kwargs["oversample"] = oversample
        super().__init__(kwargs)


class ScoreSort(AttrDict[Any]):
    """
    :arg order:
    """

    order: Union[Literal["asc", "desc"], DefaultType]

    def __init__(
        self,
        *,
        order: Union[Literal["asc", "desc"], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if order is not DEFAULT:
            kwargs["order"] = order
        super().__init__(kwargs)


class Script(AttrDict[Any]):
    """
    :arg source: The script source.
    :arg id: The `id` for a stored script.
    :arg params: Specifies any named parameters that are passed into the
        script as variables. Use parameters instead of hard-coded values
        to decrease compile time.
    :arg lang: Specifies the language the script is written in. Defaults
        to `painless` if omitted.
    :arg options:
    """

    source: Union[str, Dict[str, Any], DefaultType]
    id: Union[str, DefaultType]
    params: Union[Mapping[str, Any], DefaultType]
    lang: Union[Literal["painless", "expression", "mustache", "java"], DefaultType]
    options: Union[Mapping[str, str], DefaultType]

    def __init__(
        self,
        *,
        source: Union[str, Dict[str, Any], DefaultType] = DEFAULT,
        id: Union[str, DefaultType] = DEFAULT,
        params: Union[Mapping[str, Any], DefaultType] = DEFAULT,
        lang: Union[
            Literal["painless", "expression", "mustache", "java"], DefaultType
        ] = DEFAULT,
        options: Union[Mapping[str, str], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if source is not DEFAULT:
            kwargs["source"] = source
        if id is not DEFAULT:
            kwargs["id"] = id
        if params is not DEFAULT:
            kwargs["params"] = params
        if lang is not DEFAULT:
            kwargs["lang"] = lang
        if options is not DEFAULT:
            kwargs["options"] = options
        super().__init__(kwargs)


class ScriptField(AttrDict[Any]):
    """
    :arg script: (required)
    :arg ignore_failure:
    """

    script: Union["Script", Dict[str, Any], DefaultType]
    ignore_failure: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        script: Union["Script", Dict[str, Any], DefaultType] = DEFAULT,
        ignore_failure: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if script is not DEFAULT:
            kwargs["script"] = script
        if ignore_failure is not DEFAULT:
            kwargs["ignore_failure"] = ignore_failure
        super().__init__(kwargs)


class ScriptSort(AttrDict[Any]):
    """
    :arg script: (required)
    :arg order:
    :arg type:
    :arg mode:
    :arg nested:
    """

    script: Union["Script", Dict[str, Any], DefaultType]
    order: Union[Literal["asc", "desc"], DefaultType]
    type: Union[Literal["string", "number", "version"], DefaultType]
    mode: Union[Literal["min", "max", "sum", "avg", "median"], DefaultType]
    nested: Union["NestedSortValue", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        script: Union["Script", Dict[str, Any], DefaultType] = DEFAULT,
        order: Union[Literal["asc", "desc"], DefaultType] = DEFAULT,
        type: Union[Literal["string", "number", "version"], DefaultType] = DEFAULT,
        mode: Union[
            Literal["min", "max", "sum", "avg", "median"], DefaultType
        ] = DEFAULT,
        nested: Union["NestedSortValue", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if script is not DEFAULT:
            kwargs["script"] = script
        if order is not DEFAULT:
            kwargs["order"] = order
        if type is not DEFAULT:
            kwargs["type"] = type
        if mode is not DEFAULT:
            kwargs["mode"] = mode
        if nested is not DEFAULT:
            kwargs["nested"] = nested
        super().__init__(kwargs)


class ScriptedHeuristic(AttrDict[Any]):
    """
    :arg script: (required)
    """

    script: Union["Script", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        script: Union["Script", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if script is not DEFAULT:
            kwargs["script"] = script
        super().__init__(kwargs)


class ShapeFieldQuery(AttrDict[Any]):
    """
    :arg indexed_shape: Queries using a pre-indexed shape.
    :arg relation: Spatial relation between the query shape and the
        document shape.
    :arg shape: Queries using an inline shape definition in GeoJSON or
        Well Known Text (WKT) format.
    """

    indexed_shape: Union["FieldLookup", Dict[str, Any], DefaultType]
    relation: Union[
        Literal["intersects", "disjoint", "within", "contains"], DefaultType
    ]
    shape: Any

    def __init__(
        self,
        *,
        indexed_shape: Union["FieldLookup", Dict[str, Any], DefaultType] = DEFAULT,
        relation: Union[
            Literal["intersects", "disjoint", "within", "contains"], DefaultType
        ] = DEFAULT,
        shape: Any = DEFAULT,
        **kwargs: Any,
    ):
        if indexed_shape is not DEFAULT:
            kwargs["indexed_shape"] = indexed_shape
        if relation is not DEFAULT:
            kwargs["relation"] = relation
        if shape is not DEFAULT:
            kwargs["shape"] = shape
        super().__init__(kwargs)


class SortOptions(AttrDict[Any]):
    """
    :arg _field: The field to use in this query.
    :arg _value: The query value for the field.
    :arg _score:
    :arg _doc:
    :arg _geo_distance:
    :arg _script:
    """

    _field: Union[str, "InstrumentedField", "DefaultType"]
    _value: Union["FieldSort", Dict[str, Any], "DefaultType"]
    _score: Union["ScoreSort", Dict[str, Any], DefaultType]
    _doc: Union["ScoreSort", Dict[str, Any], DefaultType]
    _geo_distance: Union["GeoDistanceSort", Dict[str, Any], DefaultType]
    _script: Union["ScriptSort", Dict[str, Any], DefaultType]

    def __init__(
        self,
        _field: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        _value: Union["FieldSort", Dict[str, Any], "DefaultType"] = DEFAULT,
        *,
        _score: Union["ScoreSort", Dict[str, Any], DefaultType] = DEFAULT,
        _doc: Union["ScoreSort", Dict[str, Any], DefaultType] = DEFAULT,
        _geo_distance: Union["GeoDistanceSort", Dict[str, Any], DefaultType] = DEFAULT,
        _script: Union["ScriptSort", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if _field is not DEFAULT:
            kwargs[str(_field)] = _value
        if _score is not DEFAULT:
            kwargs["_score"] = _score
        if _doc is not DEFAULT:
            kwargs["_doc"] = _doc
        if _geo_distance is not DEFAULT:
            kwargs["_geo_distance"] = _geo_distance
        if _script is not DEFAULT:
            kwargs["_script"] = _script
        super().__init__(kwargs)


class SourceFilter(AttrDict[Any]):
    """
    :arg exclude_vectors: If `true`, vector fields are excluded from the
        returned source.  This option takes precedence over `includes`:
        any vector field will remain excluded even if it matches an
        `includes` rule.
    :arg excludes: A list of fields to exclude from the returned source.
    :arg includes: A list of fields to include in the returned source.
    """

    exclude_vectors: Union[bool, DefaultType]
    excludes: Union[
        Union[str, InstrumentedField],
        Sequence[Union[str, InstrumentedField]],
        DefaultType,
    ]
    includes: Union[
        Union[str, InstrumentedField],
        Sequence[Union[str, InstrumentedField]],
        DefaultType,
    ]

    def __init__(
        self,
        *,
        exclude_vectors: Union[bool, DefaultType] = DEFAULT,
        excludes: Union[
            Union[str, InstrumentedField],
            Sequence[Union[str, InstrumentedField]],
            DefaultType,
        ] = DEFAULT,
        includes: Union[
            Union[str, InstrumentedField],
            Sequence[Union[str, InstrumentedField]],
            DefaultType,
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if exclude_vectors is not DEFAULT:
            kwargs["exclude_vectors"] = exclude_vectors
        if excludes is not DEFAULT:
            kwargs["excludes"] = str(excludes)
        if includes is not DEFAULT:
            kwargs["includes"] = str(includes)
        super().__init__(kwargs)


class SpanContainingQuery(AttrDict[Any]):
    """
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

    big: Union["SpanQuery", Dict[str, Any], DefaultType]
    little: Union["SpanQuery", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        big: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        little: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if big is not DEFAULT:
            kwargs["big"] = big
        if little is not DEFAULT:
            kwargs["little"] = little
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanFieldMaskingQuery(AttrDict[Any]):
    """
    :arg field: (required)
    :arg query: (required)
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    field: Union[str, InstrumentedField, DefaultType]
    query: Union["SpanQuery", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        query: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if query is not DEFAULT:
            kwargs["query"] = query
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanFirstQuery(AttrDict[Any]):
    """
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

    end: Union[int, DefaultType]
    match: Union["SpanQuery", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        end: Union[int, DefaultType] = DEFAULT,
        match: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if end is not DEFAULT:
            kwargs["end"] = end
        if match is not DEFAULT:
            kwargs["match"] = match
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanMultiTermQuery(AttrDict[Any]):
    """
    :arg match: (required) Should be a multi term query (one of
        `wildcard`, `fuzzy`, `prefix`, `range`, or `regexp` query).
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    match: Union[Query, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        match: Union[Query, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if match is not DEFAULT:
            kwargs["match"] = match
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanNearQuery(AttrDict[Any]):
    """
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

    clauses: Union[Sequence["SpanQuery"], Sequence[Dict[str, Any]], DefaultType]
    in_order: Union[bool, DefaultType]
    slop: Union[int, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        clauses: Union[
            Sequence["SpanQuery"], Sequence[Dict[str, Any]], DefaultType
        ] = DEFAULT,
        in_order: Union[bool, DefaultType] = DEFAULT,
        slop: Union[int, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if clauses is not DEFAULT:
            kwargs["clauses"] = clauses
        if in_order is not DEFAULT:
            kwargs["in_order"] = in_order
        if slop is not DEFAULT:
            kwargs["slop"] = slop
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanNotQuery(AttrDict[Any]):
    """
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

    exclude: Union["SpanQuery", Dict[str, Any], DefaultType]
    include: Union["SpanQuery", Dict[str, Any], DefaultType]
    dist: Union[int, DefaultType]
    post: Union[int, DefaultType]
    pre: Union[int, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        exclude: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        include: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        dist: Union[int, DefaultType] = DEFAULT,
        post: Union[int, DefaultType] = DEFAULT,
        pre: Union[int, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if exclude is not DEFAULT:
            kwargs["exclude"] = exclude
        if include is not DEFAULT:
            kwargs["include"] = include
        if dist is not DEFAULT:
            kwargs["dist"] = dist
        if post is not DEFAULT:
            kwargs["post"] = post
        if pre is not DEFAULT:
            kwargs["pre"] = pre
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanOrQuery(AttrDict[Any]):
    """
    :arg clauses: (required) Array of one or more other span type queries.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    clauses: Union[Sequence["SpanQuery"], Sequence[Dict[str, Any]], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        clauses: Union[
            Sequence["SpanQuery"], Sequence[Dict[str, Any]], DefaultType
        ] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if clauses is not DEFAULT:
            kwargs["clauses"] = clauses
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanQuery(AttrDict[Any]):
    """
    :arg span_containing: Accepts a list of span queries, but only returns
        those spans which also match a second span query.
    :arg span_field_masking: Allows queries like `span_near` or `span_or`
        across different fields.
    :arg span_first: Accepts another span query whose matches must appear
        within the first N positions of the field.
    :arg span_gap:
    :arg span_multi: Wraps a `term`, `range`, `prefix`, `wildcard`,
        `regexp`, or `fuzzy` query.
    :arg span_near: Accepts multiple span queries whose matches must be
        within the specified distance of each other, and possibly in the
        same order.
    :arg span_not: Wraps another span query, and excludes any documents
        which match that query.
    :arg span_or: Combines multiple span queries and returns documents
        which match any of the specified queries.
    :arg span_term: The equivalent of the `term` query but for use with
        other span queries.
    :arg span_within: The result from a single span query is returned as
        long is its span falls within the spans returned by a list of
        other span queries.
    """

    span_containing: Union["SpanContainingQuery", Dict[str, Any], DefaultType]
    span_field_masking: Union["SpanFieldMaskingQuery", Dict[str, Any], DefaultType]
    span_first: Union["SpanFirstQuery", Dict[str, Any], DefaultType]
    span_gap: Union[Mapping[Union[str, InstrumentedField], int], DefaultType]
    span_multi: Union["SpanMultiTermQuery", Dict[str, Any], DefaultType]
    span_near: Union["SpanNearQuery", Dict[str, Any], DefaultType]
    span_not: Union["SpanNotQuery", Dict[str, Any], DefaultType]
    span_or: Union["SpanOrQuery", Dict[str, Any], DefaultType]
    span_term: Union[
        Mapping[Union[str, InstrumentedField], "SpanTermQuery"],
        Dict[str, Any],
        DefaultType,
    ]
    span_within: Union["SpanWithinQuery", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        span_containing: Union[
            "SpanContainingQuery", Dict[str, Any], DefaultType
        ] = DEFAULT,
        span_field_masking: Union[
            "SpanFieldMaskingQuery", Dict[str, Any], DefaultType
        ] = DEFAULT,
        span_first: Union["SpanFirstQuery", Dict[str, Any], DefaultType] = DEFAULT,
        span_gap: Union[
            Mapping[Union[str, InstrumentedField], int], DefaultType
        ] = DEFAULT,
        span_multi: Union["SpanMultiTermQuery", Dict[str, Any], DefaultType] = DEFAULT,
        span_near: Union["SpanNearQuery", Dict[str, Any], DefaultType] = DEFAULT,
        span_not: Union["SpanNotQuery", Dict[str, Any], DefaultType] = DEFAULT,
        span_or: Union["SpanOrQuery", Dict[str, Any], DefaultType] = DEFAULT,
        span_term: Union[
            Mapping[Union[str, InstrumentedField], "SpanTermQuery"],
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        span_within: Union["SpanWithinQuery", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if span_containing is not DEFAULT:
            kwargs["span_containing"] = span_containing
        if span_field_masking is not DEFAULT:
            kwargs["span_field_masking"] = span_field_masking
        if span_first is not DEFAULT:
            kwargs["span_first"] = span_first
        if span_gap is not DEFAULT:
            kwargs["span_gap"] = str(span_gap)
        if span_multi is not DEFAULT:
            kwargs["span_multi"] = span_multi
        if span_near is not DEFAULT:
            kwargs["span_near"] = span_near
        if span_not is not DEFAULT:
            kwargs["span_not"] = span_not
        if span_or is not DEFAULT:
            kwargs["span_or"] = span_or
        if span_term is not DEFAULT:
            kwargs["span_term"] = str(span_term)
        if span_within is not DEFAULT:
            kwargs["span_within"] = span_within
        super().__init__(kwargs)


class SpanTermQuery(AttrDict[Any]):
    """
    :arg value: (required)
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    value: Union[int, float, str, bool, None, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        value: Union[int, float, str, bool, None, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if value is not DEFAULT:
            kwargs["value"] = value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SpanWithinQuery(AttrDict[Any]):
    """
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

    big: Union["SpanQuery", Dict[str, Any], DefaultType]
    little: Union["SpanQuery", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        big: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        little: Union["SpanQuery", Dict[str, Any], DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if big is not DEFAULT:
            kwargs["big"] = big
        if little is not DEFAULT:
            kwargs["little"] = little
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class SparseVectorIndexOptions(AttrDict[Any]):
    """
    :arg prune: Whether to perform pruning, omitting the non-significant
        tokens from the query to improve query performance. If prune is
        true but the pruning_config is not specified, pruning will occur
        but default values will be used. Default: false
    :arg pruning_config: Optional pruning configuration. If enabled, this
        will omit non-significant tokens from the query in order to
        improve query performance. This is only used if prune is set to
        true. If prune is set to true but pruning_config is not specified,
        default values will be used.
    """

    prune: Union[bool, DefaultType]
    pruning_config: Union["TokenPruningConfig", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        prune: Union[bool, DefaultType] = DEFAULT,
        pruning_config: Union[
            "TokenPruningConfig", Dict[str, Any], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if prune is not DEFAULT:
            kwargs["prune"] = prune
        if pruning_config is not DEFAULT:
            kwargs["pruning_config"] = pruning_config
        super().__init__(kwargs)


class SuggestContext(AttrDict[Any]):
    """
    :arg name: (required)
    :arg type: (required)
    :arg path:
    :arg precision:
    """

    name: Union[str, DefaultType]
    type: Union[str, DefaultType]
    path: Union[str, InstrumentedField, DefaultType]
    precision: Union[int, str, DefaultType]

    def __init__(
        self,
        *,
        name: Union[str, DefaultType] = DEFAULT,
        type: Union[str, DefaultType] = DEFAULT,
        path: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        precision: Union[int, str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if name is not DEFAULT:
            kwargs["name"] = name
        if type is not DEFAULT:
            kwargs["type"] = type
        if path is not DEFAULT:
            kwargs["path"] = str(path)
        if precision is not DEFAULT:
            kwargs["precision"] = precision
        super().__init__(kwargs)


class TDigest(AttrDict[Any]):
    """
    :arg compression: Limits the maximum number of nodes used by the
        underlying TDigest algorithm to `20 * compression`, enabling
        control of memory usage and approximation error.
    :arg execution_hint: The default implementation of TDigest is
        optimized for performance, scaling to millions or even billions of
        sample values while maintaining acceptable accuracy levels (close
        to 1% relative error for millions of samples in some cases). To
        use an implementation optimized for accuracy, set this parameter
        to high_accuracy instead. Defaults to `default` if omitted.
    """

    compression: Union[int, DefaultType]
    execution_hint: Union[Literal["default", "high_accuracy"], DefaultType]

    def __init__(
        self,
        *,
        compression: Union[int, DefaultType] = DEFAULT,
        execution_hint: Union[
            Literal["default", "high_accuracy"], DefaultType
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if compression is not DEFAULT:
            kwargs["compression"] = compression
        if execution_hint is not DEFAULT:
            kwargs["execution_hint"] = execution_hint
        super().__init__(kwargs)


class TermQuery(AttrDict[Any]):
    """
    :arg value: (required) Term you wish to find in the provided field.
    :arg case_insensitive: Allows ASCII case insensitive matching of the
        value with the indexed field values when set to `true`. When
        `false`, the case sensitivity of matching depends on the
        underlying field’s mapping.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    value: Union[int, float, str, bool, None, DefaultType]
    case_insensitive: Union[bool, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        value: Union[int, float, str, bool, None, DefaultType] = DEFAULT,
        case_insensitive: Union[bool, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if value is not DEFAULT:
            kwargs["value"] = value
        if case_insensitive is not DEFAULT:
            kwargs["case_insensitive"] = case_insensitive
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class TermsLookup(AttrDict[Any]):
    """
    :arg index: (required)
    :arg id: (required)
    :arg path: (required)
    :arg routing:
    """

    index: Union[str, DefaultType]
    id: Union[str, DefaultType]
    path: Union[str, InstrumentedField, DefaultType]
    routing: Union[str, DefaultType]

    def __init__(
        self,
        *,
        index: Union[str, DefaultType] = DEFAULT,
        id: Union[str, DefaultType] = DEFAULT,
        path: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        routing: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if index is not DEFAULT:
            kwargs["index"] = index
        if id is not DEFAULT:
            kwargs["id"] = id
        if path is not DEFAULT:
            kwargs["path"] = str(path)
        if routing is not DEFAULT:
            kwargs["routing"] = routing
        super().__init__(kwargs)


class TermsPartition(AttrDict[Any]):
    """
    :arg num_partitions: (required) The number of partitions.
    :arg partition: (required) The partition number for this request.
    """

    num_partitions: Union[int, DefaultType]
    partition: Union[int, DefaultType]

    def __init__(
        self,
        *,
        num_partitions: Union[int, DefaultType] = DEFAULT,
        partition: Union[int, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if num_partitions is not DEFAULT:
            kwargs["num_partitions"] = num_partitions
        if partition is not DEFAULT:
            kwargs["partition"] = partition
        super().__init__(kwargs)


class TermsSetQuery(AttrDict[Any]):
    """
    :arg terms: (required) Array of terms you wish to find in the provided
        field.
    :arg minimum_should_match: Specification describing number of matching
        terms required to return a document.
    :arg minimum_should_match_field: Numeric field containing the number
        of matching terms required to return a document.
    :arg minimum_should_match_script: Custom script containing the number
        of matching terms required to return a document.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    terms: Union[Sequence[Union[int, float, str, bool, None]], DefaultType]
    minimum_should_match: Union[int, str, DefaultType]
    minimum_should_match_field: Union[str, InstrumentedField, DefaultType]
    minimum_should_match_script: Union["Script", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        terms: Union[
            Sequence[Union[int, float, str, bool, None]], DefaultType
        ] = DEFAULT,
        minimum_should_match: Union[int, str, DefaultType] = DEFAULT,
        minimum_should_match_field: Union[
            str, InstrumentedField, DefaultType
        ] = DEFAULT,
        minimum_should_match_script: Union[
            "Script", Dict[str, Any], DefaultType
        ] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if terms is not DEFAULT:
            kwargs["terms"] = terms
        if minimum_should_match is not DEFAULT:
            kwargs["minimum_should_match"] = minimum_should_match
        if minimum_should_match_field is not DEFAULT:
            kwargs["minimum_should_match_field"] = str(minimum_should_match_field)
        if minimum_should_match_script is not DEFAULT:
            kwargs["minimum_should_match_script"] = minimum_should_match_script
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class TestPopulation(AttrDict[Any]):
    """
    :arg field: (required) The field to aggregate.
    :arg script:
    :arg filter: A filter used to define a set of records to run unpaired
        t-test on.
    """

    field: Union[str, InstrumentedField, DefaultType]
    script: Union["Script", Dict[str, Any], DefaultType]
    filter: Union[Query, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        script: Union["Script", Dict[str, Any], DefaultType] = DEFAULT,
        filter: Union[Query, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if script is not DEFAULT:
            kwargs["script"] = script
        if filter is not DEFAULT:
            kwargs["filter"] = filter
        super().__init__(kwargs)


class TextEmbedding(AttrDict[Any]):
    """
    :arg model_id: (required)
    :arg model_text: (required)
    """

    model_id: Union[str, DefaultType]
    model_text: Union[str, DefaultType]

    def __init__(
        self,
        *,
        model_id: Union[str, DefaultType] = DEFAULT,
        model_text: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if model_id is not DEFAULT:
            kwargs["model_id"] = model_id
        if model_text is not DEFAULT:
            kwargs["model_text"] = model_text
        super().__init__(kwargs)


class TextExpansionQuery(AttrDict[Any]):
    """
    :arg model_id: (required) The text expansion NLP model to use
    :arg model_text: (required) The query text
    :arg pruning_config: Token pruning configurations
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    model_id: Union[str, DefaultType]
    model_text: Union[str, DefaultType]
    pruning_config: Union["TokenPruningConfig", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        model_id: Union[str, DefaultType] = DEFAULT,
        model_text: Union[str, DefaultType] = DEFAULT,
        pruning_config: Union[
            "TokenPruningConfig", Dict[str, Any], DefaultType
        ] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if model_id is not DEFAULT:
            kwargs["model_id"] = model_id
        if model_text is not DEFAULT:
            kwargs["model_text"] = model_text
        if pruning_config is not DEFAULT:
            kwargs["pruning_config"] = pruning_config
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class TextIndexPrefixes(AttrDict[Any]):
    """
    :arg max_chars: (required)
    :arg min_chars: (required)
    """

    max_chars: Union[int, DefaultType]
    min_chars: Union[int, DefaultType]

    def __init__(
        self,
        *,
        max_chars: Union[int, DefaultType] = DEFAULT,
        min_chars: Union[int, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if max_chars is not DEFAULT:
            kwargs["max_chars"] = max_chars
        if min_chars is not DEFAULT:
            kwargs["min_chars"] = min_chars
        super().__init__(kwargs)


class TokenPruningConfig(AttrDict[Any]):
    """
    :arg tokens_freq_ratio_threshold: Tokens whose frequency is more than
        this threshold times the average frequency of all tokens in the
        specified field are considered outliers and pruned. Defaults to
        `5` if omitted.
    :arg tokens_weight_threshold: Tokens whose weight is less than this
        threshold are considered nonsignificant and pruned. Defaults to
        `0.4` if omitted.
    :arg only_score_pruned_tokens: Whether to only score pruned tokens, vs
        only scoring kept tokens.
    """

    tokens_freq_ratio_threshold: Union[int, DefaultType]
    tokens_weight_threshold: Union[float, DefaultType]
    only_score_pruned_tokens: Union[bool, DefaultType]

    def __init__(
        self,
        *,
        tokens_freq_ratio_threshold: Union[int, DefaultType] = DEFAULT,
        tokens_weight_threshold: Union[float, DefaultType] = DEFAULT,
        only_score_pruned_tokens: Union[bool, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if tokens_freq_ratio_threshold is not DEFAULT:
            kwargs["tokens_freq_ratio_threshold"] = tokens_freq_ratio_threshold
        if tokens_weight_threshold is not DEFAULT:
            kwargs["tokens_weight_threshold"] = tokens_weight_threshold
        if only_score_pruned_tokens is not DEFAULT:
            kwargs["only_score_pruned_tokens"] = only_score_pruned_tokens
        super().__init__(kwargs)


class TopLeftBottomRightGeoBounds(AttrDict[Any]):
    """
    :arg top_left: (required)
    :arg bottom_right: (required)
    """

    top_left: Union[
        "LatLonGeoLocation",
        "GeoHashLocation",
        Sequence[float],
        str,
        Dict[str, Any],
        DefaultType,
    ]
    bottom_right: Union[
        "LatLonGeoLocation",
        "GeoHashLocation",
        Sequence[float],
        str,
        Dict[str, Any],
        DefaultType,
    ]

    def __init__(
        self,
        *,
        top_left: Union[
            "LatLonGeoLocation",
            "GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        bottom_right: Union[
            "LatLonGeoLocation",
            "GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if top_left is not DEFAULT:
            kwargs["top_left"] = top_left
        if bottom_right is not DEFAULT:
            kwargs["bottom_right"] = bottom_right
        super().__init__(kwargs)


class TopMetricsValue(AttrDict[Any]):
    """
    :arg field: (required) A field to return as a metric.
    """

    field: Union[str, InstrumentedField, DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        super().__init__(kwargs)


class TopRightBottomLeftGeoBounds(AttrDict[Any]):
    """
    :arg top_right: (required)
    :arg bottom_left: (required)
    """

    top_right: Union[
        "LatLonGeoLocation",
        "GeoHashLocation",
        Sequence[float],
        str,
        Dict[str, Any],
        DefaultType,
    ]
    bottom_left: Union[
        "LatLonGeoLocation",
        "GeoHashLocation",
        Sequence[float],
        str,
        Dict[str, Any],
        DefaultType,
    ]

    def __init__(
        self,
        *,
        top_right: Union[
            "LatLonGeoLocation",
            "GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        bottom_left: Union[
            "LatLonGeoLocation",
            "GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            DefaultType,
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if top_right is not DEFAULT:
            kwargs["top_right"] = top_right
        if bottom_left is not DEFAULT:
            kwargs["bottom_left"] = bottom_left
        super().__init__(kwargs)


class WeightedAverageValue(AttrDict[Any]):
    """
    :arg field: The field from which to extract the values or weights.
    :arg missing: A value or weight to use if the field is missing.
    :arg script:
    """

    field: Union[str, InstrumentedField, DefaultType]
    missing: Union[float, DefaultType]
    script: Union["Script", Dict[str, Any], DefaultType]

    def __init__(
        self,
        *,
        field: Union[str, InstrumentedField, DefaultType] = DEFAULT,
        missing: Union[float, DefaultType] = DEFAULT,
        script: Union["Script", Dict[str, Any], DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if field is not DEFAULT:
            kwargs["field"] = str(field)
        if missing is not DEFAULT:
            kwargs["missing"] = missing
        if script is not DEFAULT:
            kwargs["script"] = script
        super().__init__(kwargs)


class WeightedTokensQuery(AttrDict[Any]):
    """
    :arg tokens: (required) The tokens representing this query
    :arg pruning_config: Token pruning configurations
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    tokens: Union[Mapping[str, float], Sequence[Mapping[str, float]], DefaultType]
    pruning_config: Union["TokenPruningConfig", Dict[str, Any], DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        tokens: Union[
            Mapping[str, float], Sequence[Mapping[str, float]], DefaultType
        ] = DEFAULT,
        pruning_config: Union[
            "TokenPruningConfig", Dict[str, Any], DefaultType
        ] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if tokens is not DEFAULT:
            kwargs["tokens"] = tokens
        if pruning_config is not DEFAULT:
            kwargs["pruning_config"] = pruning_config
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class WildcardQuery(AttrDict[Any]):
    """
    :arg case_insensitive: Allows case insensitive matching of the pattern
        with the indexed field values when set to true. Default is false
        which means the case sensitivity of matching depends on the
        underlying field’s mapping.
    :arg rewrite: Method used to rewrite the query.
    :arg value: Wildcard pattern for terms you wish to find in the
        provided field. Required, when wildcard is not set.
    :arg wildcard: Wildcard pattern for terms you wish to find in the
        provided field. Required, when value is not set.
    :arg boost: Floating point number used to decrease or increase the
        relevance scores of the query. Boost values are relative to the
        default value of 1.0. A boost value between 0 and 1.0 decreases
        the relevance score. A value greater than 1.0 increases the
        relevance score. Defaults to `1` if omitted.
    :arg _name:
    """

    case_insensitive: Union[bool, DefaultType]
    rewrite: Union[str, DefaultType]
    value: Union[str, DefaultType]
    wildcard: Union[str, DefaultType]
    boost: Union[float, DefaultType]
    _name: Union[str, DefaultType]

    def __init__(
        self,
        *,
        case_insensitive: Union[bool, DefaultType] = DEFAULT,
        rewrite: Union[str, DefaultType] = DEFAULT,
        value: Union[str, DefaultType] = DEFAULT,
        wildcard: Union[str, DefaultType] = DEFAULT,
        boost: Union[float, DefaultType] = DEFAULT,
        _name: Union[str, DefaultType] = DEFAULT,
        **kwargs: Any,
    ):
        if case_insensitive is not DEFAULT:
            kwargs["case_insensitive"] = case_insensitive
        if rewrite is not DEFAULT:
            kwargs["rewrite"] = rewrite
        if value is not DEFAULT:
            kwargs["value"] = value
        if wildcard is not DEFAULT:
            kwargs["wildcard"] = wildcard
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if _name is not DEFAULT:
            kwargs["_name"] = _name
        super().__init__(kwargs)


class WktGeoBounds(AttrDict[Any]):
    """
    :arg wkt: (required)
    """

    wkt: Union[str, DefaultType]

    def __init__(self, *, wkt: Union[str, DefaultType] = DEFAULT, **kwargs: Any):
        if wkt is not DEFAULT:
            kwargs["wkt"] = wkt
        super().__init__(kwargs)


class AdjacencyMatrixAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["AdjacencyMatrixBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "AdjacencyMatrixBucket"]:
        return self.buckets  # type: ignore[return-value]


class AdjacencyMatrixBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: str
    doc_count: int


class AggregationBreakdown(AttrDict[Any]):
    """
    :arg build_aggregation: (required)
    :arg build_aggregation_count: (required)
    :arg build_leaf_collector: (required)
    :arg build_leaf_collector_count: (required)
    :arg collect: (required)
    :arg collect_count: (required)
    :arg initialize: (required)
    :arg initialize_count: (required)
    :arg reduce: (required)
    :arg reduce_count: (required)
    :arg post_collection:
    :arg post_collection_count:
    """

    build_aggregation: int
    build_aggregation_count: int
    build_leaf_collector: int
    build_leaf_collector_count: int
    collect: int
    collect_count: int
    initialize: int
    initialize_count: int
    reduce: int
    reduce_count: int
    post_collection: int
    post_collection_count: int


class AggregationProfile(AttrDict[Any]):
    """
    :arg breakdown: (required)
    :arg description: (required)
    :arg time_in_nanos: (required)
    :arg type: (required)
    :arg debug:
    :arg children:
    """

    breakdown: "AggregationBreakdown"
    description: str
    time_in_nanos: Any
    type: str
    debug: "AggregationProfileDebug"
    children: Sequence["AggregationProfile"]


class AggregationProfileDebug(AttrDict[Any]):
    """
    :arg segments_with_multi_valued_ords:
    :arg collection_strategy:
    :arg segments_with_single_valued_ords:
    :arg total_buckets:
    :arg built_buckets:
    :arg result_strategy:
    :arg has_filter:
    :arg delegate:
    :arg delegate_debug:
    :arg chars_fetched:
    :arg extract_count:
    :arg extract_ns:
    :arg values_fetched:
    :arg collect_analyzed_ns:
    :arg collect_analyzed_count:
    :arg surviving_buckets:
    :arg ordinals_collectors_used:
    :arg ordinals_collectors_overhead_too_high:
    :arg string_hashing_collectors_used:
    :arg numeric_collectors_used:
    :arg empty_collectors_used:
    :arg deferred_aggregators:
    :arg segments_with_doc_count_field:
    :arg segments_with_deleted_docs:
    :arg filters:
    :arg segments_counted:
    :arg segments_collected:
    :arg map_reducer:
    :arg brute_force_used:
    :arg dynamic_pruning_attempted:
    :arg dynamic_pruning_used:
    :arg skipped_due_to_no_data:
    """

    segments_with_multi_valued_ords: int
    collection_strategy: str
    segments_with_single_valued_ords: int
    total_buckets: int
    built_buckets: int
    result_strategy: str
    has_filter: bool
    delegate: str
    delegate_debug: "AggregationProfileDebug"
    chars_fetched: int
    extract_count: int
    extract_ns: int
    values_fetched: int
    collect_analyzed_ns: int
    collect_analyzed_count: int
    surviving_buckets: int
    ordinals_collectors_used: int
    ordinals_collectors_overhead_too_high: int
    string_hashing_collectors_used: int
    numeric_collectors_used: int
    empty_collectors_used: int
    deferred_aggregators: Sequence[str]
    segments_with_doc_count_field: int
    segments_with_deleted_docs: int
    filters: Sequence["AggregationProfileDelegateDebugFilter"]
    segments_counted: int
    segments_collected: int
    map_reducer: str
    brute_force_used: int
    dynamic_pruning_attempted: int
    dynamic_pruning_used: int
    skipped_due_to_no_data: int


class AggregationProfileDelegateDebugFilter(AttrDict[Any]):
    """
    :arg results_from_metadata:
    :arg query:
    :arg specialized_for:
    :arg segments_counted_in_constant_time:
    """

    results_from_metadata: int
    query: str
    specialized_for: str
    segments_counted_in_constant_time: int


class ArrayPercentilesItem(AttrDict[Any]):
    """
    :arg key: (required)
    :arg value: (required)
    :arg value_as_string:
    """

    key: float
    value: Union[float, None]
    value_as_string: str


class AutoDateHistogramAggregate(AttrDict[Any]):
    """
    :arg interval: (required)
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    interval: str
    buckets: Sequence["DateHistogramBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "DateHistogramBucket"]:
        return self.buckets  # type: ignore[return-value]


class AvgAggregate(AttrDict[Any]):
    """
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class BoxPlotAggregate(AttrDict[Any]):
    """
    :arg min: (required)
    :arg max: (required)
    :arg q1: (required)
    :arg q2: (required)
    :arg q3: (required)
    :arg lower: (required)
    :arg upper: (required)
    :arg min_as_string:
    :arg max_as_string:
    :arg q1_as_string:
    :arg q2_as_string:
    :arg q3_as_string:
    :arg lower_as_string:
    :arg upper_as_string:
    :arg meta:
    """

    min: float
    max: float
    q1: float
    q2: float
    q3: float
    lower: float
    upper: float
    min_as_string: str
    max_as_string: str
    q1_as_string: str
    q2_as_string: str
    q3_as_string: str
    lower_as_string: str
    upper_as_string: str
    meta: Mapping[str, Any]


class BucketMetricValueAggregate(AttrDict[Any]):
    """
    :arg keys: (required)
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    keys: Sequence[str]  # type: ignore[assignment]
    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class BulkIndexByScrollFailure(AttrDict[Any]):
    """
    :arg cause: (required)
    :arg id: (required)
    :arg index: (required)
    :arg status: (required)
    """

    cause: "ErrorCause"
    id: str
    index: str
    status: int


class CardinalityAggregate(AttrDict[Any]):
    """
    :arg value: (required)
    :arg meta:
    """

    value: int
    meta: Mapping[str, Any]


class ChildrenAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class ClusterDetails(AttrDict[Any]):
    """
    :arg status: (required)
    :arg indices: (required)
    :arg timed_out: (required)
    :arg took:
    :arg _shards:
    :arg failures:
    """

    status: Literal["running", "successful", "partial", "skipped", "failed"]
    indices: str
    timed_out: bool
    took: Any
    _shards: "ShardStatistics"
    failures: Sequence["ShardFailure"]


class ClusterStatistics(AttrDict[Any]):
    """
    :arg skipped: (required)
    :arg successful: (required)
    :arg total: (required)
    :arg running: (required)
    :arg partial: (required)
    :arg failed: (required)
    :arg details:
    """

    skipped: int
    successful: int
    total: int
    running: int
    partial: int
    failed: int
    details: Mapping[str, "ClusterDetails"]


class Collector(AttrDict[Any]):
    """
    :arg name: (required)
    :arg reason: (required)
    :arg time_in_nanos: (required)
    :arg children:
    """

    name: str
    reason: str
    time_in_nanos: Any
    children: Sequence["Collector"]


class CompletionSuggest(AttrDict[Any]):
    """
    :arg options: (required)
    :arg length: (required)
    :arg offset: (required)
    :arg text: (required)
    """

    options: Sequence["CompletionSuggestOption"]
    length: int
    offset: int
    text: str


class CompletionSuggestOption(AttrDict[Any]):
    """
    :arg text: (required)
    :arg collate_match:
    :arg contexts:
    :arg fields:
    :arg _id:
    :arg _index:
    :arg _routing:
    :arg _score:
    :arg _source:
    :arg score:
    """

    text: str
    collate_match: bool
    contexts: Mapping[
        str,
        Sequence[
            Union[
                str, Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str]
            ]
        ],
    ]
    fields: Mapping[str, Any]
    _id: str
    _index: str
    _routing: str
    _score: float
    _source: Any
    score: float


class CompositeAggregate(AttrDict[Any]):
    """
    :arg after_key:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    after_key: Mapping[str, Union[int, float, str, bool, None]]
    buckets: Sequence["CompositeBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "CompositeBucket"]:
        return self.buckets  # type: ignore[return-value]


class CompositeBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: Mapping[str, Union[int, float, str, bool, None]]
    doc_count: int


class CumulativeCardinalityAggregate(AttrDict[Any]):
    """
    Result of the `cumulative_cardinality` aggregation

    :arg value: (required)
    :arg value_as_string:
    :arg meta:
    """

    value: int
    value_as_string: str
    meta: Mapping[str, Any]


class DateHistogramAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["DateHistogramBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "DateHistogramBucket"]:
        return self.buckets  # type: ignore[return-value]


class DateHistogramBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    """

    key: Any
    doc_count: int
    key_as_string: str


class DateRangeAggregate(AttrDict[Any]):
    """
    Result of a `date_range` aggregation. Same format as a for a `range`
    aggregation: `from` and `to` in `buckets` are milliseconds since the
    Epoch, represented as a floating point number.

    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["RangeBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "RangeBucket"]:
        return self.buckets  # type: ignore[return-value]


class DerivativeAggregate(AttrDict[Any]):
    """
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg normalized_value:
    :arg normalized_value_as_string:
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    normalized_value: float
    normalized_value_as_string: str
    value_as_string: str
    meta: Mapping[str, Any]


class DfsKnnProfile(AttrDict[Any]):
    """
    :arg query: (required)
    :arg rewrite_time: (required)
    :arg collector: (required)
    :arg vector_operations_count:
    """

    query: Sequence["KnnQueryProfileResult"]
    rewrite_time: int
    collector: Sequence["KnnCollectorResult"]
    vector_operations_count: int


class DfsProfile(AttrDict[Any]):
    """
    :arg statistics:
    :arg knn:
    """

    statistics: "DfsStatisticsProfile"
    knn: Sequence["DfsKnnProfile"]


class DfsStatisticsBreakdown(AttrDict[Any]):
    """
    :arg collection_statistics: (required)
    :arg collection_statistics_count: (required)
    :arg create_weight: (required)
    :arg create_weight_count: (required)
    :arg rewrite: (required)
    :arg rewrite_count: (required)
    :arg term_statistics: (required)
    :arg term_statistics_count: (required)
    """

    collection_statistics: int
    collection_statistics_count: int
    create_weight: int
    create_weight_count: int
    rewrite: int
    rewrite_count: int
    term_statistics: int
    term_statistics_count: int


class DfsStatisticsProfile(AttrDict[Any]):
    """
    :arg type: (required)
    :arg description: (required)
    :arg time_in_nanos: (required)
    :arg breakdown: (required)
    :arg time:
    :arg debug:
    :arg children:
    """

    type: str
    description: str
    time_in_nanos: Any
    breakdown: "DfsStatisticsBreakdown"
    time: Any
    debug: Mapping[str, Any]
    children: Sequence["DfsStatisticsProfile"]


class DoubleTermsAggregate(AttrDict[Any]):
    """
    Result of a `terms` aggregation when the field is some kind of decimal
    number like a float, double, or distance.

    :arg doc_count_error_upper_bound:
    :arg sum_other_doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    doc_count_error_upper_bound: int
    sum_other_doc_count: int
    buckets: Sequence["DoubleTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "DoubleTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class DoubleTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    :arg doc_count_error_upper_bound:
    """

    key: float
    doc_count: int
    key_as_string: str
    doc_count_error_upper_bound: int


class ErrorCause(AttrDict[Any]):
    """
    Cause and details about a request failure. This class defines the
    properties common to all error types. Additional details are also
    provided, that depend on the error type.

    :arg type: (required) The type of error
    :arg reason: A human-readable explanation of the error, in English.
    :arg stack_trace: The server stack trace. Present only if the
        `error_trace=true` parameter was sent with the request.
    :arg caused_by:
    :arg root_cause:
    :arg suppressed:
    """

    type: str
    reason: Union[str, None]
    stack_trace: str
    caused_by: "ErrorCause"
    root_cause: Sequence["ErrorCause"]
    suppressed: Sequence["ErrorCause"]


class Explanation(AttrDict[Any]):
    """
    :arg description: (required)
    :arg details: (required)
    :arg value: (required)
    """

    description: str
    details: Sequence["ExplanationDetail"]
    value: float


class ExplanationDetail(AttrDict[Any]):
    """
    :arg description: (required)
    :arg value: (required)
    :arg details:
    """

    description: str
    value: float
    details: Sequence["ExplanationDetail"]


class ExtendedStatsAggregate(AttrDict[Any]):
    """
    :arg sum_of_squares: (required)
    :arg variance: (required)
    :arg variance_population: (required)
    :arg variance_sampling: (required)
    :arg std_deviation: (required)
    :arg std_deviation_population: (required)
    :arg std_deviation_sampling: (required)
    :arg count: (required)
    :arg min: (required)
    :arg max: (required)
    :arg avg: (required)
    :arg sum: (required)
    :arg std_deviation_bounds:
    :arg sum_of_squares_as_string:
    :arg variance_as_string:
    :arg variance_population_as_string:
    :arg variance_sampling_as_string:
    :arg std_deviation_as_string:
    :arg std_deviation_bounds_as_string:
    :arg min_as_string:
    :arg max_as_string:
    :arg avg_as_string:
    :arg sum_as_string:
    :arg meta:
    """

    sum_of_squares: Union[float, None]
    variance: Union[float, None]
    variance_population: Union[float, None]
    variance_sampling: Union[float, None]
    std_deviation: Union[float, None]
    std_deviation_population: Union[float, None]
    std_deviation_sampling: Union[float, None]
    count: int
    min: Union[float, None]
    max: Union[float, None]
    avg: Union[float, None]
    sum: float
    std_deviation_bounds: "StandardDeviationBounds"
    sum_of_squares_as_string: str
    variance_as_string: str
    variance_population_as_string: str
    variance_sampling_as_string: str
    std_deviation_as_string: str
    std_deviation_bounds_as_string: "StandardDeviationBoundsAsString"
    min_as_string: str
    max_as_string: str
    avg_as_string: str
    sum_as_string: str
    meta: Mapping[str, Any]


class ExtendedStatsBucketAggregate(AttrDict[Any]):
    """
    :arg sum_of_squares: (required)
    :arg variance: (required)
    :arg variance_population: (required)
    :arg variance_sampling: (required)
    :arg std_deviation: (required)
    :arg std_deviation_population: (required)
    :arg std_deviation_sampling: (required)
    :arg count: (required)
    :arg min: (required)
    :arg max: (required)
    :arg avg: (required)
    :arg sum: (required)
    :arg std_deviation_bounds:
    :arg sum_of_squares_as_string:
    :arg variance_as_string:
    :arg variance_population_as_string:
    :arg variance_sampling_as_string:
    :arg std_deviation_as_string:
    :arg std_deviation_bounds_as_string:
    :arg min_as_string:
    :arg max_as_string:
    :arg avg_as_string:
    :arg sum_as_string:
    :arg meta:
    """

    sum_of_squares: Union[float, None]
    variance: Union[float, None]
    variance_population: Union[float, None]
    variance_sampling: Union[float, None]
    std_deviation: Union[float, None]
    std_deviation_population: Union[float, None]
    std_deviation_sampling: Union[float, None]
    count: int
    min: Union[float, None]
    max: Union[float, None]
    avg: Union[float, None]
    sum: float
    std_deviation_bounds: "StandardDeviationBounds"
    sum_of_squares_as_string: str
    variance_as_string: str
    variance_population_as_string: str
    variance_sampling_as_string: str
    std_deviation_as_string: str
    std_deviation_bounds_as_string: "StandardDeviationBoundsAsString"
    min_as_string: str
    max_as_string: str
    avg_as_string: str
    sum_as_string: str
    meta: Mapping[str, Any]


class FetchProfile(AttrDict[Any]):
    """
    :arg type: (required)
    :arg description: (required)
    :arg time_in_nanos: (required)
    :arg breakdown: (required)
    :arg debug:
    :arg children:
    """

    type: str
    description: str
    time_in_nanos: Any
    breakdown: "FetchProfileBreakdown"
    debug: "FetchProfileDebug"
    children: Sequence["FetchProfile"]


class FetchProfileBreakdown(AttrDict[Any]):
    """
    :arg load_source:
    :arg load_source_count:
    :arg load_stored_fields:
    :arg load_stored_fields_count:
    :arg next_reader:
    :arg next_reader_count:
    :arg process_count:
    :arg process:
    """

    load_source: int
    load_source_count: int
    load_stored_fields: int
    load_stored_fields_count: int
    next_reader: int
    next_reader_count: int
    process_count: int
    process: int


class FetchProfileDebug(AttrDict[Any]):
    """
    :arg stored_fields:
    :arg fast_path:
    """

    stored_fields: Sequence[str]
    fast_path: int


class FilterAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class FiltersAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["FiltersBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "FiltersBucket"]:
        return self.buckets  # type: ignore[return-value]


class FiltersBucket(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg key:
    """

    doc_count: int
    key: str


class FrequentItemSetsAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["FrequentItemSetsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "FrequentItemSetsBucket"]:
        return self.buckets  # type: ignore[return-value]


class FrequentItemSetsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg support: (required)
    :arg doc_count: (required)
    """

    key: Mapping[str, Sequence[str]]
    support: float
    doc_count: int


class GeoBoundsAggregate(AttrDict[Any]):
    """
    :arg bounds:
    :arg meta:
    """

    bounds: Union[
        "CoordsGeoBounds",
        "TopLeftBottomRightGeoBounds",
        "TopRightBottomLeftGeoBounds",
        "WktGeoBounds",
    ]
    meta: Mapping[str, Any]


class GeoCentroidAggregate(AttrDict[Any]):
    """
    :arg count: (required)
    :arg location:
    :arg meta:
    """

    count: int
    location: Union["LatLonGeoLocation", "GeoHashLocation", Sequence[float], str]
    meta: Mapping[str, Any]


class GeoDistanceAggregate(AttrDict[Any]):
    """
    Result of a `geo_distance` aggregation. The unit for `from` and `to`
    is meters by default.

    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["RangeBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "RangeBucket"]:
        return self.buckets  # type: ignore[return-value]


class GeoHashGridAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["GeoHashGridBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "GeoHashGridBucket"]:
        return self.buckets  # type: ignore[return-value]


class GeoHashGridBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: str
    doc_count: int


class GeoHexGridAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["GeoHexGridBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "GeoHexGridBucket"]:
        return self.buckets  # type: ignore[return-value]


class GeoHexGridBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: str
    doc_count: int


class GeoLine(AttrDict[Any]):
    """
    A GeoJson GeoLine.

    :arg type: (required) Always `"LineString"`
    :arg coordinates: (required) Array of `[lon, lat]` coordinates
    """

    type: str
    coordinates: Sequence[Sequence[float]]


class GeoLineAggregate(AttrDict[Any]):
    """
    :arg type: (required)
    :arg geometry: (required)
    :arg properties: (required)
    :arg meta:
    """

    type: str
    geometry: "GeoLine"
    properties: Any
    meta: Mapping[str, Any]


class GeoTileGridAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["GeoTileGridBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "GeoTileGridBucket"]:
        return self.buckets  # type: ignore[return-value]


class GeoTileGridBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: str
    doc_count: int


class GlobalAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class HdrPercentileRanksAggregate(AttrDict[Any]):
    """
    :arg values: (required)
    :arg meta:
    """

    values: Union[
        Mapping[str, Union[str, float, None]], Sequence["ArrayPercentilesItem"]
    ]
    meta: Mapping[str, Any]


class HdrPercentilesAggregate(AttrDict[Any]):
    """
    :arg values: (required)
    :arg meta:
    """

    values: Union[
        Mapping[str, Union[str, float, None]], Sequence["ArrayPercentilesItem"]
    ]
    meta: Mapping[str, Any]


class HistogramAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["HistogramBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "HistogramBucket"]:
        return self.buckets  # type: ignore[return-value]


class HistogramBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    """

    key: float
    doc_count: int
    key_as_string: str


class Hit(AttrDict[Any]):
    """
    :arg index: (required)
    :arg id:
    :arg score:
    :arg explanation:
    :arg fields:
    :arg highlight:
    :arg inner_hits:
    :arg matched_queries:
    :arg nested:
    :arg ignored:
    :arg ignored_field_values:
    :arg shard:
    :arg node:
    :arg routing:
    :arg source:
    :arg rank:
    :arg seq_no:
    :arg primary_term:
    :arg version:
    :arg sort:
    """

    index: str
    id: str
    score: Union[float, None]
    explanation: "Explanation"
    fields: Mapping[str, Any]
    highlight: Mapping[str, Sequence[str]]
    inner_hits: Mapping[str, "InnerHitsResult"]
    matched_queries: Union[Sequence[str], Mapping[str, float]]
    nested: "NestedIdentity"
    ignored: Sequence[str]
    ignored_field_values: Mapping[str, Sequence[Any]]
    shard: str
    node: str
    routing: str
    source: Any
    rank: int
    seq_no: int
    primary_term: int
    version: int
    sort: Sequence[Union[int, float, str, bool, None]]


class HitsMetadata(AttrDict[Any]):
    """
    :arg hits: (required)
    :arg total: Total hit count information, present only if
        `track_total_hits` wasn't `false` in the search request.
    :arg max_score:
    """

    hits: Sequence["Hit"]
    total: Union["TotalHits", int]
    max_score: Union[float, None]


class InferenceAggregate(AttrDict[Any]):
    """
    :arg value:
    :arg feature_importance:
    :arg top_classes:
    :arg warning:
    :arg meta:
    """

    value: Union[int, float, str, bool, None]
    feature_importance: Sequence["InferenceFeatureImportance"]
    top_classes: Sequence["InferenceTopClassEntry"]
    warning: str
    meta: Mapping[str, Any]


class InferenceClassImportance(AttrDict[Any]):
    """
    :arg class_name: (required)
    :arg importance: (required)
    """

    class_name: str
    importance: float


class InferenceFeatureImportance(AttrDict[Any]):
    """
    :arg feature_name: (required)
    :arg importance:
    :arg classes:
    """

    feature_name: str
    importance: float
    classes: Sequence["InferenceClassImportance"]


class InferenceTopClassEntry(AttrDict[Any]):
    """
    :arg class_name: (required)
    :arg class_probability: (required)
    :arg class_score: (required)
    """

    class_name: Union[int, float, str, bool, None]
    class_probability: float
    class_score: float


class InnerHitsResult(AttrDict[Any]):
    """
    :arg hits: (required)
    """

    hits: "HitsMetadata"


class IpPrefixAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["IpPrefixBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "IpPrefixBucket"]:
        return self.buckets  # type: ignore[return-value]


class IpPrefixBucket(AttrDict[Any]):
    """
    :arg is_ipv6: (required)
    :arg key: (required)
    :arg prefix_length: (required)
    :arg doc_count: (required)
    :arg netmask:
    """

    is_ipv6: bool
    key: str
    prefix_length: int
    doc_count: int
    netmask: str


class IpRangeAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["IpRangeBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "IpRangeBucket"]:
        return self.buckets  # type: ignore[return-value]


class IpRangeBucket(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg key:
    :arg from:
    :arg to:
    """

    doc_count: int
    key: str
    from_: str
    to: str


class KnnCollectorResult(AttrDict[Any]):
    """
    :arg name: (required)
    :arg reason: (required)
    :arg time_in_nanos: (required)
    :arg time:
    :arg children:
    """

    name: str
    reason: str
    time_in_nanos: Any
    time: Any
    children: Sequence["KnnCollectorResult"]


class KnnQueryProfileBreakdown(AttrDict[Any]):
    """
    :arg advance: (required)
    :arg advance_count: (required)
    :arg build_scorer: (required)
    :arg build_scorer_count: (required)
    :arg compute_max_score: (required)
    :arg compute_max_score_count: (required)
    :arg count_weight: (required)
    :arg count_weight_count: (required)
    :arg create_weight: (required)
    :arg create_weight_count: (required)
    :arg match: (required)
    :arg match_count: (required)
    :arg next_doc: (required)
    :arg next_doc_count: (required)
    :arg score: (required)
    :arg score_count: (required)
    :arg set_min_competitive_score: (required)
    :arg set_min_competitive_score_count: (required)
    :arg shallow_advance: (required)
    :arg shallow_advance_count: (required)
    """

    advance: int
    advance_count: int
    build_scorer: int
    build_scorer_count: int
    compute_max_score: int
    compute_max_score_count: int
    count_weight: int
    count_weight_count: int
    create_weight: int
    create_weight_count: int
    match: int
    match_count: int
    next_doc: int
    next_doc_count: int
    score: int
    score_count: int
    set_min_competitive_score: int
    set_min_competitive_score_count: int
    shallow_advance: int
    shallow_advance_count: int


class KnnQueryProfileResult(AttrDict[Any]):
    """
    :arg type: (required)
    :arg description: (required)
    :arg time_in_nanos: (required)
    :arg breakdown: (required)
    :arg time:
    :arg debug:
    :arg children:
    """

    type: str
    description: str
    time_in_nanos: Any
    breakdown: "KnnQueryProfileBreakdown"
    time: Any
    debug: Mapping[str, Any]
    children: Sequence["KnnQueryProfileResult"]


class LongRareTermsAggregate(AttrDict[Any]):
    """
    Result of the `rare_terms` aggregation when the field is some kind of
    whole number like a integer, long, or a date.

    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["LongRareTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "LongRareTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class LongRareTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    """

    key: int
    doc_count: int
    key_as_string: str


class LongTermsAggregate(AttrDict[Any]):
    """
    Result of a `terms` aggregation when the field is some kind of whole
    number like a integer, long, or a date.

    :arg doc_count_error_upper_bound:
    :arg sum_other_doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    doc_count_error_upper_bound: int
    sum_other_doc_count: int
    buckets: Sequence["LongTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "LongTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class LongTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    :arg doc_count_error_upper_bound:
    """

    key: int
    doc_count: int
    key_as_string: str
    doc_count_error_upper_bound: int


class MatrixStatsAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg fields:
    :arg meta:
    """

    doc_count: int
    fields: Sequence["MatrixStatsFields"]
    meta: Mapping[str, Any]


class MatrixStatsFields(AttrDict[Any]):
    """
    :arg name: (required)
    :arg count: (required)
    :arg mean: (required)
    :arg variance: (required)
    :arg skewness: (required)
    :arg kurtosis: (required)
    :arg covariance: (required)
    :arg correlation: (required)
    """

    name: str
    count: int
    mean: float
    variance: float
    skewness: float
    kurtosis: float
    covariance: Mapping[str, float]
    correlation: Mapping[str, float]


class MaxAggregate(AttrDict[Any]):
    """
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class MedianAbsoluteDeviationAggregate(AttrDict[Any]):
    """
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class MinAggregate(AttrDict[Any]):
    """
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class MissingAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class MultiTermsAggregate(AttrDict[Any]):
    """
    :arg doc_count_error_upper_bound:
    :arg sum_other_doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    doc_count_error_upper_bound: int
    sum_other_doc_count: int
    buckets: Sequence["MultiTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "MultiTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class MultiTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    :arg doc_count_error_upper_bound:
    """

    key: Sequence[Union[int, float, str, bool, None]]
    doc_count: int
    key_as_string: str
    doc_count_error_upper_bound: int


class NestedAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class NestedIdentity(AttrDict[Any]):
    """
    :arg field: (required)
    :arg offset: (required)
    :arg _nested:
    """

    field: str
    offset: int
    _nested: "NestedIdentity"


class ParentAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class PercentilesBucketAggregate(AttrDict[Any]):
    """
    :arg values: (required)
    :arg meta:
    """

    values: Union[
        Mapping[str, Union[str, float, None]], Sequence["ArrayPercentilesItem"]
    ]
    meta: Mapping[str, Any]


class PhraseSuggest(AttrDict[Any]):
    """
    :arg options: (required)
    :arg length: (required)
    :arg offset: (required)
    :arg text: (required)
    """

    options: Sequence["PhraseSuggestOption"]
    length: int
    offset: int
    text: str


class PhraseSuggestOption(AttrDict[Any]):
    """
    :arg text: (required)
    :arg score: (required)
    :arg highlighted:
    :arg collate_match:
    """

    text: str
    score: float
    highlighted: str
    collate_match: bool


class Profile(AttrDict[Any]):
    """
    :arg shards: (required)
    """

    shards: Sequence["ShardProfile"]


class QueryBreakdown(AttrDict[Any]):
    """
    :arg advance: (required)
    :arg advance_count: (required)
    :arg build_scorer: (required)
    :arg build_scorer_count: (required)
    :arg create_weight: (required)
    :arg create_weight_count: (required)
    :arg match: (required)
    :arg match_count: (required)
    :arg shallow_advance: (required)
    :arg shallow_advance_count: (required)
    :arg next_doc: (required)
    :arg next_doc_count: (required)
    :arg score: (required)
    :arg score_count: (required)
    :arg compute_max_score: (required)
    :arg compute_max_score_count: (required)
    :arg count_weight: (required)
    :arg count_weight_count: (required)
    :arg set_min_competitive_score: (required)
    :arg set_min_competitive_score_count: (required)
    """

    advance: int
    advance_count: int
    build_scorer: int
    build_scorer_count: int
    create_weight: int
    create_weight_count: int
    match: int
    match_count: int
    shallow_advance: int
    shallow_advance_count: int
    next_doc: int
    next_doc_count: int
    score: int
    score_count: int
    compute_max_score: int
    compute_max_score_count: int
    count_weight: int
    count_weight_count: int
    set_min_competitive_score: int
    set_min_competitive_score_count: int


class QueryProfile(AttrDict[Any]):
    """
    :arg breakdown: (required)
    :arg description: (required)
    :arg time_in_nanos: (required)
    :arg type: (required)
    :arg children:
    """

    breakdown: "QueryBreakdown"
    description: str
    time_in_nanos: Any
    type: str
    children: Sequence["QueryProfile"]


class RangeAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["RangeBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "RangeBucket"]:
        return self.buckets  # type: ignore[return-value]


class RangeBucket(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg from:
    :arg to:
    :arg from_as_string:
    :arg to_as_string:
    :arg key: The bucket key. Present if the aggregation is _not_ keyed
    """

    doc_count: int
    from_: float
    to: float
    from_as_string: str
    to_as_string: str
    key: str


class RateAggregate(AttrDict[Any]):
    """
    :arg value: (required)
    :arg value_as_string:
    :arg meta:
    """

    value: float
    value_as_string: str
    meta: Mapping[str, Any]


class Retries(AttrDict[Any]):
    """
    :arg bulk: (required) The number of bulk actions retried.
    :arg search: (required) The number of search actions retried.
    """

    bulk: int
    search: int


class ReverseNestedAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class SamplerAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class ScriptedMetricAggregate(AttrDict[Any]):
    """
    :arg value: (required)
    :arg meta:
    """

    value: Any
    meta: Mapping[str, Any]


class SearchProfile(AttrDict[Any]):
    """
    :arg collector: (required)
    :arg query: (required)
    :arg rewrite_time: (required)
    """

    collector: Sequence["Collector"]
    query: Sequence["QueryProfile"]
    rewrite_time: int


class ShardFailure(AttrDict[Any]):
    """
    :arg reason: (required)
    :arg index:
    :arg node:
    :arg shard:
    :arg status:
    :arg primary:
    """

    reason: "ErrorCause"
    index: str
    node: str
    shard: int
    status: str
    primary: bool


class ShardProfile(AttrDict[Any]):
    """
    :arg aggregations: (required)
    :arg cluster: (required)
    :arg id: (required)
    :arg index: (required)
    :arg node_id: (required)
    :arg searches: (required)
    :arg shard_id: (required)
    :arg dfs:
    :arg fetch:
    """

    aggregations: Sequence["AggregationProfile"]
    cluster: str
    id: str
    index: str
    node_id: str
    searches: Sequence["SearchProfile"]
    shard_id: int
    dfs: "DfsProfile"
    fetch: "FetchProfile"


class ShardStatistics(AttrDict[Any]):
    """
    :arg failed: (required) The number of shards the operation or search
        attempted to run on but failed.
    :arg successful: (required) The number of shards the operation or
        search succeeded on.
    :arg total: (required) The number of shards the operation or search
        will run on overall.
    :arg failures:
    :arg skipped:
    """

    failed: int
    successful: int
    total: int
    failures: Sequence["ShardFailure"]
    skipped: int


class SignificantLongTermsAggregate(AttrDict[Any]):
    """
    :arg bg_count:
    :arg doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    bg_count: int
    doc_count: int
    buckets: Sequence["SignificantLongTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "SignificantLongTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class SignificantLongTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg score: (required)
    :arg bg_count: (required)
    :arg doc_count: (required)
    :arg key_as_string:
    """

    key: int
    score: float
    bg_count: int
    doc_count: int
    key_as_string: str


class SignificantStringTermsAggregate(AttrDict[Any]):
    """
    :arg bg_count:
    :arg doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    bg_count: int
    doc_count: int
    buckets: Sequence["SignificantStringTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "SignificantStringTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class SignificantStringTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg score: (required)
    :arg bg_count: (required)
    :arg doc_count: (required)
    """

    key: str
    score: float
    bg_count: int
    doc_count: int


class SimpleValueAggregate(AttrDict[Any]):
    """
    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class StandardDeviationBounds(AttrDict[Any]):
    """
    :arg upper: (required)
    :arg lower: (required)
    :arg upper_population: (required)
    :arg lower_population: (required)
    :arg upper_sampling: (required)
    :arg lower_sampling: (required)
    """

    upper: Union[float, None]
    lower: Union[float, None]
    upper_population: Union[float, None]
    lower_population: Union[float, None]
    upper_sampling: Union[float, None]
    lower_sampling: Union[float, None]


class StandardDeviationBoundsAsString(AttrDict[Any]):
    """
    :arg upper: (required)
    :arg lower: (required)
    :arg upper_population: (required)
    :arg lower_population: (required)
    :arg upper_sampling: (required)
    :arg lower_sampling: (required)
    """

    upper: str
    lower: str
    upper_population: str
    lower_population: str
    upper_sampling: str
    lower_sampling: str


class StatsAggregate(AttrDict[Any]):
    """
    Statistics aggregation result. `min`, `max` and `avg` are missing if
    there were no values to process (`count` is zero).

    :arg count: (required)
    :arg min: (required)
    :arg max: (required)
    :arg avg: (required)
    :arg sum: (required)
    :arg min_as_string:
    :arg max_as_string:
    :arg avg_as_string:
    :arg sum_as_string:
    :arg meta:
    """

    count: int
    min: Union[float, None]
    max: Union[float, None]
    avg: Union[float, None]
    sum: float
    min_as_string: str
    max_as_string: str
    avg_as_string: str
    sum_as_string: str
    meta: Mapping[str, Any]


class StatsBucketAggregate(AttrDict[Any]):
    """
    :arg count: (required)
    :arg min: (required)
    :arg max: (required)
    :arg avg: (required)
    :arg sum: (required)
    :arg min_as_string:
    :arg max_as_string:
    :arg avg_as_string:
    :arg sum_as_string:
    :arg meta:
    """

    count: int
    min: Union[float, None]
    max: Union[float, None]
    avg: Union[float, None]
    sum: float
    min_as_string: str
    max_as_string: str
    avg_as_string: str
    sum_as_string: str
    meta: Mapping[str, Any]


class StringRareTermsAggregate(AttrDict[Any]):
    """
    Result of the `rare_terms` aggregation when the field is a string.

    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["StringRareTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "StringRareTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class StringRareTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: str
    doc_count: int


class StringStatsAggregate(AttrDict[Any]):
    """
    :arg count: (required)
    :arg min_length: (required)
    :arg max_length: (required)
    :arg avg_length: (required)
    :arg entropy: (required)
    :arg distribution:
    :arg min_length_as_string:
    :arg max_length_as_string:
    :arg avg_length_as_string:
    :arg meta:
    """

    count: int
    min_length: Union[int, None]
    max_length: Union[int, None]
    avg_length: Union[float, None]
    entropy: Union[float, None]
    distribution: Union[Mapping[str, float], None]
    min_length_as_string: str
    max_length_as_string: str
    avg_length_as_string: str
    meta: Mapping[str, Any]


class StringTermsAggregate(AttrDict[Any]):
    """
    Result of a `terms` aggregation when the field is a string.

    :arg doc_count_error_upper_bound:
    :arg sum_other_doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    doc_count_error_upper_bound: int
    sum_other_doc_count: int
    buckets: Sequence["StringTermsBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "StringTermsBucket"]:
        return self.buckets  # type: ignore[return-value]


class StringTermsBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    :arg doc_count_error_upper_bound:
    """

    key: Union[int, float, str, bool, None]
    doc_count: int
    doc_count_error_upper_bound: int


class SumAggregate(AttrDict[Any]):
    """
    Sum aggregation result. `value` is always present and is zero if there
    were no values to process.

    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class TDigestPercentileRanksAggregate(AttrDict[Any]):
    """
    :arg values: (required)
    :arg meta:
    """

    values: Union[
        Mapping[str, Union[str, float, None]], Sequence["ArrayPercentilesItem"]
    ]
    meta: Mapping[str, Any]


class TDigestPercentilesAggregate(AttrDict[Any]):
    """
    :arg values: (required)
    :arg meta:
    """

    values: Union[
        Mapping[str, Union[str, float, None]], Sequence["ArrayPercentilesItem"]
    ]
    meta: Mapping[str, Any]


class TTestAggregate(AttrDict[Any]):
    """
    :arg value: (required)
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class TermSuggest(AttrDict[Any]):
    """
    :arg options: (required)
    :arg length: (required)
    :arg offset: (required)
    :arg text: (required)
    """

    options: Sequence["TermSuggestOption"]
    length: int
    offset: int
    text: str


class TermSuggestOption(AttrDict[Any]):
    """
    :arg text: (required)
    :arg score: (required)
    :arg freq: (required)
    :arg highlighted:
    :arg collate_match:
    """

    text: str
    score: float
    freq: int
    highlighted: str
    collate_match: bool


class TimeSeriesAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["TimeSeriesBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "TimeSeriesBucket"]:
        return self.buckets  # type: ignore[return-value]


class TimeSeriesBucket(AttrDict[Any]):
    """
    :arg key: (required)
    :arg doc_count: (required)
    """

    key: Mapping[str, Union[int, float, str, bool, None]]
    doc_count: int


class TopHitsAggregate(AttrDict[Any]):
    """
    :arg hits: (required)
    :arg meta:
    """

    hits: "HitsMetadata"
    meta: Mapping[str, Any]


class TopMetrics(AttrDict[Any]):
    """
    :arg sort: (required)
    :arg metrics: (required)
    """

    sort: Sequence[Union[Union[int, float, str, bool, None], None]]
    metrics: Mapping[str, Union[Union[int, float, str, bool, None], None]]


class TopMetricsAggregate(AttrDict[Any]):
    """
    :arg top: (required)
    :arg meta:
    """

    top: Sequence["TopMetrics"]
    meta: Mapping[str, Any]


class TotalHits(AttrDict[Any]):
    """
    :arg relation: (required)
    :arg value: (required)
    """

    relation: Literal["eq", "gte"]
    value: int


class UnmappedRareTermsAggregate(AttrDict[Any]):
    """
    Result of a `rare_terms` aggregation when the field is unmapped.
    `buckets` is always empty.

    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence[Any]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, Any]:
        return self.buckets  # type: ignore[return-value]


class UnmappedSamplerAggregate(AttrDict[Any]):
    """
    :arg doc_count: (required)
    :arg meta:
    """

    doc_count: int
    meta: Mapping[str, Any]


class UnmappedSignificantTermsAggregate(AttrDict[Any]):
    """
    Result of the `significant_terms` aggregation on an unmapped field.
    `buckets` is always empty.

    :arg bg_count:
    :arg doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    bg_count: int
    doc_count: int
    buckets: Sequence[Any]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, Any]:
        return self.buckets  # type: ignore[return-value]


class UnmappedTermsAggregate(AttrDict[Any]):
    """
    Result of a `terms` aggregation when the field is unmapped. `buckets`
    is always empty.

    :arg doc_count_error_upper_bound:
    :arg sum_other_doc_count:
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    doc_count_error_upper_bound: int
    sum_other_doc_count: int
    buckets: Sequence[Any]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, Any]:
        return self.buckets  # type: ignore[return-value]


class ValueCountAggregate(AttrDict[Any]):
    """
    Value count aggregation result. `value` is always present.

    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]


class VariableWidthHistogramAggregate(AttrDict[Any]):
    """
    :arg buckets: (required) the aggregation buckets as a list
    :arg meta:
    """

    buckets: Sequence["VariableWidthHistogramBucket"]
    meta: Mapping[str, Any]

    @property
    def buckets_as_dict(self) -> Mapping[str, "VariableWidthHistogramBucket"]:
        return self.buckets  # type: ignore[return-value]


class VariableWidthHistogramBucket(AttrDict[Any]):
    """
    :arg min: (required)
    :arg key: (required)
    :arg max: (required)
    :arg doc_count: (required)
    :arg min_as_string:
    :arg key_as_string:
    :arg max_as_string:
    """

    min: float
    key: float
    max: float
    doc_count: int
    min_as_string: str
    key_as_string: str
    max_as_string: str


class WeightedAvgAggregate(AttrDict[Any]):
    """
    Weighted average aggregation result. `value` is missing if the weight
    was set to zero.

    :arg value: (required) The metric value. A missing value generally
        means that there was no data to aggregate, unless specified
        otherwise.
    :arg value_as_string:
    :arg meta:
    """

    value: Union[float, None]
    value_as_string: str
    meta: Mapping[str, Any]
