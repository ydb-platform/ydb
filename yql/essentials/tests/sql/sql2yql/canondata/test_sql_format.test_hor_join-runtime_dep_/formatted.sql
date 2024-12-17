/* postgres can not */
USE plato;

INSERT INTO @ttt WITH TRUNCATE
SELECT
    CAST(key AS int) AS key,
    subkey,
    value
FROM
    Input
;

COMMIT;

SELECT
    *
FROM (
    SELECT
        key,
        '' AS value
    FROM
        @ttt
    UNION ALL
    SELECT
        0 AS key,
        value
    FROM
        @ttt
) AS x
ORDER BY
    key,
    value
;
