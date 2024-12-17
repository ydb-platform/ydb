/* syntax version 1 */
/* postgres can not */
USE plato;

$keep_only_last = ($row) -> {
    $members = ListFilter(StructMembers($row), ($x) -> (FIND($x, 'key') IS NOT NULL));
    RETURN ChooseMembers($row, $members);
};

SELECT
    *
FROM (
    SELECT
        $keep_only_last(TableRow())
    FROM
        Input
);
