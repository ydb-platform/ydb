/* postgres can not */
SELECT
    key,
    Math::Sqrt(CAST(row_number() OVER w AS double)) AS sq
FROM
    plato.Input
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            subkey
    )
ORDER BY
    key,
    sq
;
