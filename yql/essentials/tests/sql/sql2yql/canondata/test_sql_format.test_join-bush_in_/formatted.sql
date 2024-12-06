PRAGMA DisableSimpleColumns;

USE plato;

SELECT
    *
FROM (
    SELECT
        i1.key AS Key,
        i1.value AS Value,
        i2.value AS Join
    FROM
        Roots AS i1
    INNER JOIN
        Leaves AS i2
    ON
        i1.leaf == i2.key
    UNION ALL
    SELECT
        i1.key AS Key,
        i1.value AS Value,
        i2.value AS Join
    FROM
        Roots AS i1
    INNER JOIN
        Branches AS i2
    ON
        i1.branch == i2.key
)
ORDER BY
    Key,
    Value,
    Join
;
