PRAGMA YqlSelect = 'force';

PRAGMA OrderedColumns;

SELECT
    *
FROM (
    SELECT
        10 AS z,
        [1, 2] AS value,
        20 AS a
)
    FLATTEN LIST BY value
ORDER BY
    value
;

SELECT
    *
FROM (
    SELECT
        10 AS z,
        {'one': 1, 'two': 2} AS value,
        20 AS a
)
    FLATTEN DICT BY value
ORDER BY
    value
;

SELECT
    *
FROM (
    SELECT
        10 AS z,
        Just(1) AS value,
        20 AS a
)
    FLATTEN OPTIONAL BY value
ORDER BY
    value
;

SELECT
    *
FROM (
    SELECT
        10 AS z,
        [1, 2] AS value,
        20 AS a
)
    FLATTEN BY value
ORDER BY
    value
;
