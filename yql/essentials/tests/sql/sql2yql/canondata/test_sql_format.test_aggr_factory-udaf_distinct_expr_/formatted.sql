/* syntax version 1 */
/* postgres can not */
-- count example
$create = ($_item, $_parent) -> {
    RETURN 1
};

$add = ($state, $_item, $_parent) -> {
    RETURN 1 + $state
};

$merge = ($state1, $state2) -> {
    RETURN $state1 + $state2
};

$get_result = ($state) -> {
    RETURN $state
};

$serialize = ($state) -> {
    RETURN $state
};

$deserialize = ($state) -> {
    RETURN $state
};

$default = 0;
$f = AGGREGATION_FACTORY("udaf", $create, $add, $merge, $get_result, $serialize, $deserialize, $default);
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a), AsStruct(1 AS a));

USE plato;

INSERT INTO @a
SELECT
    *
FROM
    as_table($t)
;

COMMIT;

SELECT
    AGGREGATE_BY(DISTINCT CAST(Unicode::ToLower(CAST(a AS Utf8) || "00"u) AS Int), $f)
FROM
    @a
;
