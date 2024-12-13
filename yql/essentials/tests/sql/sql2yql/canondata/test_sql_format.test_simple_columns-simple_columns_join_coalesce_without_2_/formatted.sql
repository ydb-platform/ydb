/* syntax version 1 */
USE plato;

PRAGMA SimpleColumns;
PRAGMA CoalesceJoinKeysOnQualifiedAll;

SELECT
    b.*
WITHOUT
    b.x
FROM (
    SELECT
        *
    FROM (
        SELECT
            AsList(1, 2, 3) AS x,
            AsList(1, 2) AS y
    )
        FLATTEN BY (
            x,
            y
        )
) AS a
JOIN (
    SELECT
        *
    FROM (
        SELECT
            AsList(1, 2, 3) AS x,
            AsList(2, 3) AS y
    )
        FLATTEN BY (
            x,
            y
        )
) AS b
ON
    a.x == b.x AND a.y == b.y
;

SELECT
    *
WITHOUT
    b.x
FROM (
    SELECT
        *
    FROM (
        SELECT
            AsList(1, 2, 3) AS x,
            AsList(1, 2) AS y
    )
        FLATTEN BY (
            x,
            y
        )
) AS a
JOIN (
    SELECT
        *
    FROM (
        SELECT
            AsList(1, 2, 3) AS x,
            AsList(2, 3) AS y
    )
        FLATTEN BY (
            x,
            y
        )
) AS b
ON
    a.x == b.x AND a.y == b.y
;
