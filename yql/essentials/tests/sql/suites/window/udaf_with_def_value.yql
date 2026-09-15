pragma warning("disable", "4520");

$my_table =
SELECT 1 AS id, 1  AS ts, 4 AS value1
UNION ALL
SELECT 2 AS id, 1  AS ts, NULL AS value1
UNION ALL
SELECT 1 AS id, 2  AS ts, 4 AS value1
UNION ALL
SELECT 3 AS id, 2  AS ts, 40 AS value1
UNION ALL
SELECT 3 AS id, 5  AS ts, 2 AS value1
UNION ALL
SELECT 3 AS id, 10 AS ts, 40 AS value1
;

$cnt_create = ($_item, $_parent) -> { return 1ul };
$cnt_add = ($state, $_item, $_parent) -> { return 1ul + $state };
$cnt_merge = ($state1, $state2) -> { return $state1 + $state2 };
$cnt_get_result = ($state) -> { return $state };
$cnt_serialize = ($state) -> { return $state };
$cnt_deserialize = ($state) -> { return $state };
-- non-trivial default value 
$cnt_default = 0.0;

$cnt_udaf_factory = AggregationFactory(
	    "UDAF",
	    $cnt_create,
	    $cnt_add,
	    $cnt_merge,
	    $cnt_get_result,
	    $cnt_serialize,
	    $cnt_deserialize,
	    $cnt_default
);


SELECT
    id
    , ts
    , value1
    , AGGREGATE_BY(value1, $cnt_udaf_factory) OVER lagging AS lagging_opt
    , AGGREGATE_BY(value1, $cnt_udaf_factory) OVER generic AS generic_opt

    , AGGREGATE_BY(ts, $cnt_udaf_factory) OVER lagging AS lagging
    , AGGREGATE_BY(ts, $cnt_udaf_factory) OVER generic AS generic

    , AGGREGATE_BY(value1, $cnt_udaf_factory) OVER empty AS empty_opt
    , AGGREGATE_BY(ts, $cnt_udaf_factory) OVER empty AS empty

FROM $my_table
WINDOW lagging AS (
	    ORDER BY ts, id
	    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
)
, generic AS (
	    ORDER BY ts, id
	    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
)
, empty AS (
	    ORDER BY ts, id
	    ROWS BETWEEN 10 FOLLOWING AND 1 FOLLOWING
)
ORDER BY ts, id;

