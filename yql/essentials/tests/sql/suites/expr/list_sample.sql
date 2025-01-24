/* yt can not */
$list = ListFromRange(1, 101);
$test = ($probability, $dependsOn) -> { 
    $sample = ListCollect(ListSample($list, $probability, $dependsOn));
    RETURN 
    (
        ListSort(DictKeys(ToSet($sample))) == ListSort($sample),
        (ListLength($sample), $probability * 100),
        SetIncludes(ToSet($list), $sample)
    );
};

SELECT
    ListSample(NULL                                               , 1.0) IS NULL AS mustBeTrue1,
    ListSample(Nothing(OptionalType(ListType(DataType("Uint64")))), 1.0) IS NULL AS mustBeTrue2,
    ListSample([]                                                 , 1.0) == []   AS mustBeTrue3,
    
    ListSample($list, NULL                                     ) == $list AS mustBeTrue4,
    ListSample($list, Nothing(OptionalType(DataType("Double")))) == $list AS mustBeTrue5,

    ListSample($list, 0.5, 123) == ListSample($list, 0.5, 123) AS mustBeTrue6,

    $test(0.2, 1) AS result1,
    $test(0.2, 2) AS result2,
    $test(0.2, 3) AS result3,
    $test(0.2, 4) AS result4,
    $test(0.2, 5) AS result5,
    $test(0.5, 6) AS result6,
    $test(0.8, 7) AS result7,
    $test(1.0,   8) AS result8,
    $test(0.0,   9) AS result9,

    ListSample($list      , 0.1      , 10) AS result10,
    ListSample(Just($list), 0.1      , 11) AS result11,
    ListSample($list      , Just(0.1), 12) AS result12,
    ListSample(Just($list), Just(0.1), 13) AS result13;
