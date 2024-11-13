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

select ListSampleN(NULL, 1ul, 1) is NULL,
       ListSampleN($list, NULL, 2) == $list,
       $test(5u, 1), $test(5u, 1),
       $test(10u, 2),
       $test(20u, 3),
       $test(0u, 4),
       $test(100u, 5),
       ListSampleN(Just($list), Just(10u), (10, 1)),
       ListSampleN(Just($list), Just(10u), (10, 2)),
       ListSampleN($list, 10u, 11),
       ListSampleN($list, 15u, 12),
       ListSampleN($list, 10u);
