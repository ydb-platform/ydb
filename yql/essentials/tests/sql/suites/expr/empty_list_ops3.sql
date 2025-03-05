/* syntax version 1 */
/* postgres can not */
$pred = ($x)->{ return $x > 0 };

select
    ListExtract([],"a"),
    ListTakeWhile([], $pred),
    ListSkipWhile([], $pred),
    ListTakeWhileInclusive([], $pred),
    ListSkipWhileInclusive([], $pred),
    ListAggregate([], AGGREGATION_FACTORY("Sum")),
    ToDict([]),
    ToMultiDict([]),
    ToSet([]),
    SetIsDisjoint({1, 2, 3}, []),
    SetIncludes({1, 2, 3}, []),
    Yql::Fold([],0,($x,$y)->{return $x+$y}),
    Yql::Fold1([],($x)->{return $x},($x,$y)->{return $x+$y}),
    Yql::FoldMap([],0,($x,$y)->{return ($x+$y,$y)}),
    Yql::Fold1Map([],($x)->{return ($x,$x)},($x,$y)->{return ($x+$y,$y)}),
    Yql::LazyList([]),

    ListExtract(Just([]),"a"),
    ListTakeWhile(Just([]), $pred),
    ListSkipWhile(Just([]), $pred),
    ListTakeWhileInclusive(Just([]), $pred),
    ListSkipWhileInclusive(Just([]), $pred),
    ListAggregate(Just([]), AGGREGATION_FACTORY("Sum")),
    ToDict(Just([])),
    ToMultiDict(Just([])),
    ToSet(Just([])),
    SetIsDisjoint({1, 2, 3}, Just([])),
    SetIncludes({1, 2, 3}, Just([])),
    