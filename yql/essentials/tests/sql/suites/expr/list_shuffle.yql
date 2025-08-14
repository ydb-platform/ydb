/* yt can not */
$list = ListFromRange(1, 40);
$test = ($c) -> { 
    $shuffle = ListCollect(ListShuffle($list, $c));
    RETURN ListSort($shuffle) == ListSort($list);
};

SELECT
    ListShuffle(NULL                                               ) IS NULL AS mustBeTrue1,
    ListShuffle(Nothing(OptionalType(ListType(DataType("Uint64"))))) IS NULL AS mustBeTrue2,
    ListShuffle([]                                                 ) == []   AS mustBeTrue3,

    ListShuffle($list, 123) == ListShuffle($list, 123) AS mustBeTrue4,

    $test(1) AS result1,
    $test(2) AS result2,
    $test(3) AS result3,
    $test(4) AS result4,

    ListShuffle($list      , 5) AS result5,
    ListShuffle(Just($list), 6) AS result6;
