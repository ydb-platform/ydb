USE plato;
PRAGMA OrderedColumns;

SELECT
    a.*,
    lag(key) OVER (
        ORDER BY
            subkey
    ) AS prev_k,
    min(key) OVER (
        ORDER BY
            subkey
    ) AS min_k
FROM
    Input AS a
ORDER BY
    subkey
;
