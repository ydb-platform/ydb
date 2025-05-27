/* syntax version 1 */
/* postgres can not */
$pred = ($x) -> {
    RETURN $x > 0;
};

SELECT
    ListExtract([], 'a'),
    ListTakeWhile([], $pred),
    ListSkipWhile([], $pred),
    ListTakeWhileInclusive([], $pred),
    ListSkipWhileInclusive([], $pred),
    ListAggregate([], AGGREGATION_FACTORY('Sum')),
    ToDict([]),
    ToMultiDict([]),
    ToSet([]),
    SetIsDisjoint({1, 2, 3}, []),
    SetIncludes({1, 2, 3}, []),
    Yql::Fold(
        [], 0, ($x, $y) -> {
            RETURN $x + $y;
        }
    ),
    Yql::Fold1(
        [], ($x) -> {
            RETURN $x;
        }, ($x, $y) -> {
            RETURN $x + $y;
        }
    ),
    Yql::FoldMap(
        [], 0, ($x, $y) -> {
            RETURN ($x + $y, $y);
        }
    ),
    Yql::Fold1Map(
        [], ($x) -> {
            RETURN ($x, $x);
        }, ($x, $y) -> {
            RETURN ($x + $y, $y);
        }
    ),
    Yql::LazyList([]),
    ListExtract(Just([]), 'a'),
    ListTakeWhile(Just([]), $pred),
    ListSkipWhile(Just([]), $pred),
    ListTakeWhileInclusive(Just([]), $pred),
    ListSkipWhileInclusive(Just([]), $pred),
    ListAggregate(Just([]), AGGREGATION_FACTORY('Sum')),
    ToDict(Just([])),
    ToMultiDict(Just([])),
    ToSet(Just([])),
    SetIsDisjoint({1, 2, 3}, Just([])),
    SetIncludes({1, 2, 3}, Just([])),
;
