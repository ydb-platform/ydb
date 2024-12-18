USE plato;

$t = (
    SELECT DISTINCT
        key
    FROM
        Input
);

SELECT
    key,
    subkey,
    max(value) AS max_val,
    grouping(a.key, a.subkey) AS g_ks,
    grouping(a.subkey, a.key) AS g_sk,
    grouping(a.key) AS g_k,
    grouping(a.subkey) AS g_s,
FROM
    Input AS a
JOIN
    $t AS b
ON
    a.key == b.key
GROUP BY
    ROLLUP (a.key, a.subkey)
;
