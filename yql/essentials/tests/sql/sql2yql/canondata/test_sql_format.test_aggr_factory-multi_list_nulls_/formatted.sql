/* syntax version 1 */
/* postgres can not */
$data = AsList(
    AsStruct(AsList(1.0, 2.0) AS x),
    AsStruct(AsList(3.0, 4.0) AS x),
    AsStruct(AsList(NULL, NULL) AS x),
    AsStruct(AsList(2.0, 3.0, 4.0) AS x),
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
