/* postgres can not */
USE plato;

PRAGMA DisableOrderedColumns;
PRAGMA warning('disable', '4517');

$Group = 1u;

INSERT INTO Output (
    Group,
    Name
)
SELECT
    $Group,
    value
FROM
    Input
WHERE
    key == '150'
LIMIT 1;
