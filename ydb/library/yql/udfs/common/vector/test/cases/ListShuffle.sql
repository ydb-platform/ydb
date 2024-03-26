$list = ListFromRange(1, 40);
$test = ($c) -> { 
    $shuffle = ListCollect(ListShuffle($list, $c));
    RETURN ListSort($shuffle) == ListSort($list);
};

select ListShuffle(NULL, 1) is NULL,
       $test(1), $test(1),
       $test(2),
       $test(3),
       $test(4),
       ListShuffle(Just($list), (6, 1)),
       ListShuffle(Just($list), (6, 2)),
       ListShuffle($list);
