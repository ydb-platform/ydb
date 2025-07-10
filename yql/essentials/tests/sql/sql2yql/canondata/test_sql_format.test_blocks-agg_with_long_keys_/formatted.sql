PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$src = ListMap(
    ListFromRange(CAST(0 AS Int64), CAST(500 AS Int64)), ($keyVal) -> {
        RETURN <|
            k1: $keyVal,
            k2: $keyVal + 1,
            k3: $keyVal + 2,
            v: $keyVal
        |>;
    }
);

SELECT
    k1,
    k2,
    k3,
    sum(v) AS s
FROM
    as_table($src)
GROUP BY
    k1,
    k2,
    k3
ORDER BY
    k1,
    k2,
    k3,
    s
;
