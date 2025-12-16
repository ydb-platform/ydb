$get_factory = ($buckets, $min, $max) -> { return AggregationFactory(
        "UDAF",
        ($item, $parent) -> { return Udf(StatisticsInternal::EquiWidthHistogramCreate, $parent as Depends)($item, $buckets, $min, $max) },
        ($state, $item, $parent) -> { return Udf(StatisticsInternal::EquiWidthHistogramAddValue, $parent as Depends)($state, $item) },
        StatisticsInternal::EquiWidthHistogramMerge,
        StatisticsInternal::EquiWidthHistogramFinalize,
        StatisticsInternal::EquiWidthHistogramSerialize,
        StatisticsInternal::EquiWidthHistogramDeserialize,
    )
};

$t1 = [
    <|key: 1, value: Date("1970-01-21")|>,
    <|key: 2, value: Date("1970-02-10")|>,
    <|key: 3, value: Date("1970-03-02")|>
];

select AGGREGATE_BY(key, $get_factory(2, 0, 2)), AGGREGATE_BY(value, $get_factory(2, 30, 50)) from AS_TABLE($t1);
