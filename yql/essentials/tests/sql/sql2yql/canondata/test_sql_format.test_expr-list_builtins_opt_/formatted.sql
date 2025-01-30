/* postgres can not */
/* syntax version 1 */
$list0 = AsList('a', 'b');
$list1 = AsList(Just('a'), 'b', Nothing(ParseType('String?')));
$list2 = Just(AsList('a', 'b'));
$list3 = Just(AsList(Just('a'), 'b', Nothing(ParseType('String?'))));
$list_empty0 = ListCreate(ParseType('String'));
$list_empty1 = ListCreate(ParseType('String?'));
$list_empty2 = Just(ListCreate(ParseType('String')));
$list_empty3 = Just(ListCreate(ParseType('String?')));
$list_null0 = Just(AsList(Nothing(ParseType('String?'))));
$list_null1 = Nothing(ParseType('List<String?>?'));
$list_min = Just(AsList(Just(Just(Just('a'))), 'b'));

SELECT
    ListConcat($list0) AS list0,
    ListConcat($list1) AS list1,
    ListConcat($list2) AS list2,
    ListConcat($list3) AS list3,
    ListConcat($list_empty0) AS list_empty0,
    ListConcat($list_empty1) AS list_empty1,
    ListConcat($list_empty2) AS list_empty2,
    ListConcat($list_empty3) AS list_empty3,
    ListConcat($list_null0) AS list_null0,
    ListConcat($list_null1) AS list_null1,
    ListMin($list_min) AS list_min
;
