#pragma once

namespace NSQLTranslationV1 {

enum EAggregationType {
    NORMAL,
    KEY_PAYLOAD,
    PAYLOAD_PREDICATE,
    TWO_ARGS,
    COUNT,
    HISTOGRAM,
    LINEAR_HISTOGRAM,
    PERCENTILE,
    TOPFREQ,
    TOP,
    TOP_BY,
    COUNT_DISTINCT_ESTIMATE,
    LIST,
    UDAF,
    PG,
    NTH_VALUE,
    RANDOM_SAMPLE,
    RANDOM_VALUE
};

enum class EAggregateMode {
    Normal,
    Distinct,
    OverWindow,
    OverWindowDistinct,
};

} // namespace NSQLTranslationV1
