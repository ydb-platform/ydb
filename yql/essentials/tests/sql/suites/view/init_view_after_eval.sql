/* postgres can not */
use plato;

$i = select * from Input;
$i = process $i;

$members = StructTypeComponents(ListItemType(TypeHandle(TypeOf($i))));
$filteredMembers = ListFilter(ListMap($members, ($x) -> { return $x.Name }), ($x) -> { return $x > "k" });

select ChooseMembers(TableRow(), $filteredMembers) from Input view raw;
