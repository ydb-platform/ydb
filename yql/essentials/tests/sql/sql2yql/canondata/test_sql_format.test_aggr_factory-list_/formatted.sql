/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a));
$f = AGGREGATION_FACTORY("aggregate_list");

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

$f = AGGREGATION_FACTORY("aggregate_list", length(CAST(Unicode::ToUpper("xx"u) AS String)));

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
    *
FROM
    as_table($t)
;

COMMIT;

SELECT
    ListSort(AGGREGATE_BY(a, $f))
FROM
    @a
;

SELECT
    ListSort(AGGREGATE_BY(DISTINCT a, $f))
FROM
    @a
;
