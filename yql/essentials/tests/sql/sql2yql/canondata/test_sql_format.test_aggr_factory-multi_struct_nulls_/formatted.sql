/* syntax version 1 */
/* postgres can not */
$data = AsList(
    AsStruct(AsStruct(1.0 AS a, 2 AS b) AS x),
    AsStruct(AsStruct(3.0 AS a, 4 AS b) AS x),
    AsStruct(AsStruct(NULL AS a, NULL AS b) AS x),
    AsStruct(AsStruct(2.0 AS a, 3 AS b, 4.0 AS c) AS x),
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
