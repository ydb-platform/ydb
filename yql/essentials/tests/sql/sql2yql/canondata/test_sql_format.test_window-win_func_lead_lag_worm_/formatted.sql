/* postgres can not */
USE plato;

$input = (
    SELECT
        CAST(key AS int32) AS key,
        CAST(subkey AS int32) AS subkey,
        value
    FROM
        Input
);

--insert into Output
SELECT
    lead(key, 3) OVER w AS keyL,
    lead(key, 2) OVER w AS keyM,
    lead(key, 1) OVER w AS keyN,
    key AS keyO,
    lag(key, 1) OVER w AS key_1,
    lag(key, 2) OVER w AS key_2,
    lag(key, 3) OVER w AS key_3,
    lag(key, 4) OVER w AS key_4,
    lag(key, 5) OVER w AS key_5,
    value
FROM
    $input
WINDOW
    w AS (
        ORDER BY
            key,
            value
    )
ORDER BY
    keyO,
    value
;
