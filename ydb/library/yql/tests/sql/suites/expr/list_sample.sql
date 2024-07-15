$list = ListFromRange(1, 101);
$test = ($r, $c) -> { 
    $sample = ListCollect(ListSample($list, $r, $c));
    RETURN 
    (
        ListSort(DictKeys(ToSet($sample))) == ListSort($sample),
        (ListLength($sample), $r * 100),
        SetIncludes(ToSet($list), $sample)
    );
};

select ListSample(NULL, 1, 1) is NULL,
       ListSample($list, NULL, 2) == $list,
       $test(0.2, 1), $test(0.2, 1),
       $test(0.2, 2),
       $test(0.2, 3),
       $test(0.2, 4),
       $test(0.2, 5),
       $test(0.5, 6),
       $test(0.8, 7),
       $test(1, 8),
       $test(0, 9),
       ListSample(Just($list), Just(0.1), (10, 1)),
       ListSample(Just($list), Just(0.1), (10, 2)),
       ListSample(Just($list), 0.1);
