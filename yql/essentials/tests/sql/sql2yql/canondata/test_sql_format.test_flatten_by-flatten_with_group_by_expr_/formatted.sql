/* syntax version 1 */
/* postgres can not */
$data_deep = (
    SELECT
        mod,
        aggregate_list(CAST(key AS uint32)) AS lk,
        aggregate_list(CAST(subkey AS uint32)) AS ls,
        Count(*) AS cc
    FROM plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

SELECT
    ss,
    sum(cc) AS sc,
    sum(mod) AS sm
FROM $data_deep
    AS d
    FLATTEN BY (
        lk AS itk,
        ls AS its
    )
GROUP BY
    its + itk AS ss
ORDER BY
    ss;
