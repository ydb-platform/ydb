/* yt can not */
$list = ListFromRange(1, 40);
$test = ($n, $dependsOn) -> { 
    $sample = ListCollect(ListSampleN($list, $n, $dependsOn));
    RETURN 
    (
        ListSort(DictKeys(ToSet($sample))) == ListSort($sample),
        ListLength($sample) == ListMin(AsList($n, ListLength($list))),
        SetIncludes(ToSet($list), $sample)
    );
};

SELECT
    ListSampleN(NULL                                               , 1ul) IS NULL AS mustBeTrue1,
    ListSampleN(Nothing(OptionalType(ListType(DataType("Uint64")))), 1ul) IS NULL AS mustBeTrue2,
    ListSampleN([]                                                 , 1ul) == []   AS mustBeTrue3,

    ListSampleN($list, NULL                                     ) == $list AS mustBeTrue4,
    ListSampleN($list, Nothing(OptionalType(DataType("Uint64")))) == $list AS mustBeTrue5,

    ListSampleN($list, 25ul, 123) == ListSampleN($list, 25ul, 123) AS mustBeTrue6,

    $test(5ul,   1) AS result1,
    $test(10ul,  2) AS result2,
    $test(20ul,  3) AS result3,
    $test(0ul,   4) AS result4,
    $test(100ul, 5) AS result5,

    ListSampleN($list      , 10ul      , 6) AS result6,
    ListSampleN(Just($list), 10ul      , 7) AS result7,
    ListSampleN($list      , Just(10ul), 8) AS result8,
    ListSampleN(Just($list), Just(10ul), 9) AS result9;
