/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a));
$f = AGGREGATION_FACTORY("loghistogram");

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
    )
;
$f = AGGREGATION_FACTORY("loghistogram", 10, 0.01, 1000.0);

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
    )
;
USE plato;

INSERT INTO @a
SELECT
    a AS aa
FROM
    as_table($t)
;
COMMIT;

SELECT
    AGGREGATE_BY(aa, $f)
FROM
    @a
;
