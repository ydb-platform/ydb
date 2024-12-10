/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a));
$f = AGGREGATION_FACTORY("linearhistogram");

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

$f = AGGREGATION_FACTORY("linearhistogram", 10, 0.0, 1000.0);

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
