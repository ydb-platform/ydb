/* syntax version 1 */
/* postgres can not */
use plato;

$keep_only_last = ($row) -> {
    $members = ListFilter(StructMembers($row), ($x) -> (FIND($x, "key") IS NOT NULL));
    return ChooseMembers($row, $members)
};


select * from 
(select $keep_only_last(TableRow()) from Input)