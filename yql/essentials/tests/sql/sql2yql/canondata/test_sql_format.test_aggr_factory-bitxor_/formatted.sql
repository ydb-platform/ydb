/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1u AS a), AsStruct(2u AS a));
$f = AGGREGATION_FACTORY('bitxor');

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom('res'), $f(
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
