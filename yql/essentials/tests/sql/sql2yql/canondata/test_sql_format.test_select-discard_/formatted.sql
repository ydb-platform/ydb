/* postgres can not */
USE plato;
PRAGMA DisableSimpleColumns;

DISCARD SELECT
    1
;

DISCARD SELECT
    *
FROM
    Input
;

DISCARD SELECT
    *
FROM
    Input
WHERE
    key < "foo"
;

DISCARD SELECT
    *
FROM
    Input AS a
JOIN
    Input AS b
USING (key);

DISCARD SELECT
    sum(length(value)),
    key,
    subkey
FROM
    Input
GROUP BY
    ROLLUP (key, subkey)
ORDER BY
    key,
    subkey
;

DISCARD SELECT
    *
FROM (
    SELECT
        key || "a" || "b" AS key
    FROM
        Input
) AS a
JOIN (
    SELECT
        key || "ab" AS key
    FROM
        Input
) AS b
USING (key);
