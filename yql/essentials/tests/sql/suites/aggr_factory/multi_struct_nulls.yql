/* syntax version 1 */
/* postgres can not */

$data = AsList(
    AsStruct(AsStruct(1.0 as a, 2 as b) as x),
    AsStruct(AsStruct(3.0 as a, 4 as b) as x),
    AsStruct(AsStruct(NULL as a, NULL as b) as x),
    AsStruct(AsStruct(2.0 as a, 3 as b, 4.0 as c) as x),
);

SELECT  
    MULTI_AGGREGATE_BY(x, AggregationFactory("agg_list")),
    MULTI_AGGREGATE_BY(x, AggregationFactory("avg")),
    MULTI_AGGREGATE_BY(x, AggregationFactory("count")),
FROM
    AS_TABLE($data);

SELECT
    MULTI_AGGREGATE_BY(x, AggregationFactory("agg_list")),
    MULTI_AGGREGATE_BY(x, AggregationFactory("avg")),
    MULTI_AGGREGATE_BY(x, AggregationFactory("count")),
FROM
    (SELECT * FROM AS_TABLE($data) LIMIT 0);
