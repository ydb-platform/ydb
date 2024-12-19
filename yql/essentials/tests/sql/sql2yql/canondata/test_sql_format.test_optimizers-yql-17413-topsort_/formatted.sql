-- ignore runonopt plan diff
USE plato;

$filtered = (
    SELECT
        *
    FROM
        Input
    WHERE
        value != 'xxx'
);

SELECT DISTINCT
    (subkey) AS subkey
FROM (
    SELECT
        *
    FROM
        $filtered
    ORDER BY
        key DESC
    LIMIT 3
)
ORDER BY
    subkey
;

SELECT
    sum(CAST(subkey AS int32)) AS c
FROM
    $filtered
;
