/* syntax version 1 */
/* postgres can not */
$t = AsList(
    AsStruct(1 AS key, 101 AS value),
    AsStruct(6 AS key, 34 AS value),
    AsStruct(4 AS key, 22 AS value),
    AsStruct(2 AS key, 256 AS value),
    AsStruct(7 AS key, 111 AS value)
);
$f = AGGREGATION_FACTORY("topby", 3);

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
