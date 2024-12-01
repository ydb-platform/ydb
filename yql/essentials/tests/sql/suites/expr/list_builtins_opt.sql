/* postgres can not */
/* syntax version 1 */

$list0 = AsList("a","b");
$list1 = AsList(Just("a"), "b", Nothing(ParseType("String?")));
$list2 = Just(AsList("a","b"));
$list3 = Just(AsList(Just("a"), "b", Nothing(ParseType("String?"))));

$list_empty0 = ListCreate(ParseType("String"));
$list_empty1 = ListCreate(ParseType("String?"));
$list_empty2 = Just(ListCreate(ParseType("String")));
$list_empty3 = Just(ListCreate(ParseType("String?")));


$list_null0 = Just(AsList(Nothing(ParseType("String?"))));
$list_null1 = Nothing(ParseType("List<String?>?"));

$list_min = Just(AsList(Just(Just(Just("a"))), "b"));


select ListConcat($list0) as list0,
       ListConcat($list1) as list1,
       ListConcat($list2) as list2,
       ListConcat($list3) as list3,

       ListConcat($list_empty0) as list_empty0,
       ListConcat($list_empty1) as list_empty1,
       ListConcat($list_empty2) as list_empty2,
       ListConcat($list_empty3) as list_empty3,
       
       ListConcat($list_null0) as list_null0,
       ListConcat($list_null1) as list_null1,

       ListMin($list_min) as list_min;
