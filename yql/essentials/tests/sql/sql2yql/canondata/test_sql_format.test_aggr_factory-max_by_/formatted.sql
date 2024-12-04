/* syntax version 1 */
/* postgres can not */
$t = AsList(
    AsStruct(1 AS key, 200 AS value),
    AsStruct(2 AS key, 100 AS value)
);
$f = AGGREGATION_FACTORY("maxby");

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"),
                $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN AsTuple($z.value, $z.key)
                    }
                )
            )
        )
    );
$f = AGGREGATION_FACTORY("maxby", 10);

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"),
                $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN AsTuple($z.value, $z.key)
                    }
                )
            )
        )
    );
USE plato;

INSERT INTO @a
SELECT
    AsTuple(value, key) AS vk
FROM as_table($t);
COMMIT;

SELECT
    AGGREGATE_BY(vk, $f)
FROM @a;
