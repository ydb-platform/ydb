$get_factory = ($width, $depth) -> { return AggregationFactory(
        "UDAF",
        ($item, $parent) -> { return Udf(StatisticsInternal::CountMinSketchCreate, $parent as Depends)($item, $width, $depth) },
        ($state, $item, $parent) -> { return Udf(StatisticsInternal::CountMinSketchAddValue, $parent as Depends)($state, $item) },
        StatisticsInternal::CountMinSketchMerge,
        StatisticsInternal::CountMinSketchFinalize,
        StatisticsInternal::CountMinSketchSerialize,
        StatisticsInternal::CountMinSketchDeserialize,
    )
};

$t1 = [
    <|key: 1, value: "v1"|>,
    <|key: 2, value: "v2"|>,
    <|key: 3, value: "v3"|>
];

select AGGREGATE_BY(key, $get_factory(2, 2)), AGGREGATE_BY(value, $get_factory(3, 3)) from AS_TABLE($t1);
