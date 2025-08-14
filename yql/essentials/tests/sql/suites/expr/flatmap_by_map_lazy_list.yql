$lazy = ListFromRange(1us, 3us);
$list = AsList("one", "two");

SELECT ListFlatMap($list, ($l)->{ RETURN ListMap($lazy, ($r)->{ RETURN AsTuple($l, $r) })});
SELECT ListFlatMap($lazy, ($l)->{ RETURN ListMap($list, ($r)->{ RETURN AsTuple($l, $r) })});

