PRAGMA DisableSimpleColumns;

/* postgres can not */
$in = (
    SELECT
        *
    FROM plato.Input
    WHERE key > "100"
);

SELECT
    *
FROM $in
    AS a
INNER JOIN $in
    AS b
ON a.key == b.key;
