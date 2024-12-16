PRAGMA DisableSimpleColumns;

SELECT
    count(*)
FROM (
    SELECT
        CAST(subkey AS int) AS v1,
        subkey
    FROM
        plato.Input1
) AS a
FULL JOIN (
    SELECT
        CAST(subkey AS int) AS v2,
        subkey
    FROM
        plato.Input1
) AS b
ON
    a.v1 == b.v2
;
