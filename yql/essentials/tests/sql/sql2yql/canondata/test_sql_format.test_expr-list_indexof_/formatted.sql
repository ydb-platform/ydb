$list = [1, 2, 3];
$opt_list = [1, NULL, 2, 3];

SELECT
    ListIndexOf($list, 2),
    ListIndexOf($list, 100),
    ListIndexOf(Just($opt_list), 2),
    ListIndexOf(Just($opt_list), 200),
    ListIndexOf(Nothing(List<Int32>?), 2),
    ListIndexOf([], 'foo'),
    ListIndexOf(NULL, 1.0),
;
