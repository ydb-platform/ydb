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
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a));

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"), $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN $z.a
                    }
                )
            )
        )
    );
$t = AsList(AsStruct(1 / 0 AS a), AsStruct(2 / 0 AS a));

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"), $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN $z.a
                    }
                )
            )
        )
    );
USE plato;

INSERT INTO @a
SELECT
    *
FROM as_table($t);
COMMIT;

SELECT
    AGGREGATE_BY(a, $f)
FROM @a;

SELECT
    AGGREGATE_BY(DISTINCT a, $f)
FROM @a;
