USE plato;

SELECT
    row_number() OVER (
        ORDER BY
            key
    ) AS rn,
    key
FROM
    Input
GROUP BY
    ROLLUP (key, subkey)
ORDER BY
    rn
;
