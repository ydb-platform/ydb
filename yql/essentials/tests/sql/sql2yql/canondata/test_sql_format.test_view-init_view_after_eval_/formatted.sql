/* postgres can not */
USE plato;

$i =
    SELECT
        *
    FROM
        Input
;

$i =
    PROCESS $i;
$members = StructTypeComponents(ListItemType(TypeHandle(TypeOf($i))));
$filteredMembers = ListFilter(
    ListMap(
        $members, ($x) -> {
            RETURN $x.Name
        }
    ), ($x) -> {
        RETURN $x > "k"
    }
);

SELECT
    ChooseMembers(TableRow(), $filteredMembers)
FROM
    Input VIEW raw
;
