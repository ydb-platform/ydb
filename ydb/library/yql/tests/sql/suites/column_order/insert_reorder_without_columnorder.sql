/* postgres can not */
use plato;
pragma DisableOrderedColumns;
pragma warning("disable", "4517");

$Group = 1u;

INSERT INTO Output(Group, Name)
SELECT
    $Group,
    value
FROM Input
WHERE key = "150"
LIMIT 1;
