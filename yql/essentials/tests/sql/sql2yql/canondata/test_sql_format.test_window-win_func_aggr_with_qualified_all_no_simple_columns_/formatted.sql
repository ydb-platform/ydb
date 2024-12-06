PRAGMA DisableSimpleColumns;
USE plato;

$q = (
    SELECT
        CAST(key AS Int32) AS key,
        CAST(subkey AS Int32) AS subkey,
        value
    FROM
        Input
);

SELECT
    t.*,
    sum(subkey) OVER w AS subkey_sum,
    sum(key) OVER w
FROM
    $q AS t
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            value
    )
ORDER BY
    `t.key`,
    `t.subkey`
;
