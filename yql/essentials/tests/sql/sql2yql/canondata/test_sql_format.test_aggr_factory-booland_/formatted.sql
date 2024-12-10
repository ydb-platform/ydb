/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(TRUE AS a), AsStruct(FALSE AS a));
$f = AGGREGATION_FACTORY("booland");

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
    AGGREGATE_BY(DISTINCT a, $f)
FROM
    @a
;
