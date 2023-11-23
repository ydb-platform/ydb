/* postgres can not */
/* skip double format */
$list = ParseFile("int32","keyid.lst");
select ListExtend(
    ListMap($list, ($x)->{return $x + 1 }),
    ListMap($list, ($x)->{return $x + 2 }));