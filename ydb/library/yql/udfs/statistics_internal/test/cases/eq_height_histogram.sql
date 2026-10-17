$get_factory = ($buckets, $emission, $maxBytes) -> { return AggregationFactory(
        "UDAF",
        ($item, $parent) -> { return Udf(StatisticsInternal::EQHCreate, $parent as Depends)($item, $buckets, $emission, $maxBytes) },
        ($state, $item, $parent) -> { return Udf(StatisticsInternal::EQHAddValue, $parent as Depends)($state, $item) },
        StatisticsInternal::EQHMerge,
        StatisticsInternal::EQHFinalize,
        StatisticsInternal::EQHSerialize,
        StatisticsInternal::EQHDeserialize,
    )
};

$t1 = [
    <|key: 1u, value: "v1"|>,
    <|key: 2u, value: "v2"|>,
    <|key: 3u, value: "v3"|>
];

-- Typed suffixes are load-bearing: a bare integer types as Int32, and
-- EQHCreate reads params with Get<ui32>() / Get<ui64>(). 2147483648ul does
-- not fit in Int32, so a missing `ul` fails this case instead of silently
-- feeding garbage into MaxStateBytes.
select
    Length(Unwrap(AGGREGATE_BY(Udf(StatisticsInternal::PresortKey)(AsTuple(key)), $get_factory(4u, 32u, 2147483648ul)))) > 0,
    Length(Unwrap(AGGREGATE_BY(Udf(StatisticsInternal::PresortKey)(AsTuple(value)), $get_factory(4u, 32u, 2147483648ul)))) > 0
from AS_TABLE($t1);
