/* postgres can not */
USE plato;
PRAGMA simplecolumns;

SELECT
    a.*,
    (ROW_NUMBER() OVER w) - 1 AS position_cnt,
    lag(key) OVER w AS pkey,
    lead(key) OVER w AS nkey
FROM Input
    AS a
WINDOW
    w AS (
        ORDER BY
            value DESC
    )
ORDER BY
    position_cnt;
