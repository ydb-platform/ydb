$list = ListFromRange(1, 40);
$test = ($n, $c) -> { 
    $sample = ListCollect(ListSampleN($list, $n, $c));
    RETURN 
    (
        ListSort(DictKeys(ToSet($sample))) == ListSort($sample),
        ListLength($sample) == ListMin(AsList($n, ListLength($list))),
        SetIncludes(ToSet($list), $sample)
    );
};

select ListSampleN(NULL, 1, 1) is NULL,
       ListSampleN($list, NULL, 2) == $list,
       $test(5, 1), $test(5, 1),
       $test(10, 2),
       $test(20, 3),
       $test(0, 4),
       $test(100, 5),
       ListSampleN(Just($list), Just(10), 10, 1),
       ListSampleN(Just($list), Just(10), 10, 2),
       ListSampleN($list, 10, 11),
       ListSampleN($list, 15, 12);
