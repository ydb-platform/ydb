PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$src = ListMap(
    ListFromRange(CAST(0 AS Int64), CAST(500 AS Int64)), ($x) -> {
        RETURN <|
            k: if($x % 10 == 0, NULL, CAST($x AS String)),
            v: $x
        |>;
    }
);

SELECT
    k,
    sum(v) AS s
FROM
    as_table($src)
GROUP BY
    k
ORDER BY
    k,
    s
;
