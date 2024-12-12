/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a));
$f = AGGREGATION_FACTORY("mode");

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"), $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN $z.a;
                    }
                )
            )
        )
    )
;

$f = AGGREGATION_FACTORY("topfreq", 10, 20);

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"), $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN $z.a;
                    }
                )
            )
        )
    )
;

USE plato;

INSERT INTO @a
SELECT
    *
FROM
    as_table($t)
;

COMMIT;

SELECT
    AGGREGATE_BY(a, $f)
FROM
    @a
;

SELECT
    ListSort(
        AGGREGATE_BY(DISTINCT a, $f), ($x) -> {
            RETURN $x.Value;
        }
    )
FROM
    @a
;
