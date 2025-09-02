/* syntax version 1 */
/* postgres can not */
$data = AsList(
    AsStruct(AsTuple(1.0, 2) AS x),
    AsStruct(AsTuple(3.0, 4) AS x),
    AsStruct(AsTuple(NULL, NULL) AS x),
    AsStruct(AsTuple(2.0, 3) AS x),
);

SELECT
    MULTI_AGGREGATE_BY(x, AggregationFactory('agg_list')),
    MULTI_AGGREGATE_BY(x, AggregationFactory('avg')),
    MULTI_AGGREGATE_BY(x, AggregationFactory('count')),
FROM
    AS_TABLE($data)
;

SELECT
    MULTI_AGGREGATE_BY(x, AggregationFactory('agg_list')),
    MULTI_AGGREGATE_BY(x, AggregationFactory('avg')),
    MULTI_AGGREGATE_BY(x, AggregationFactory('count')),
FROM (
    SELECT
        *
    FROM
        AS_TABLE($data)
    LIMIT 0
);
