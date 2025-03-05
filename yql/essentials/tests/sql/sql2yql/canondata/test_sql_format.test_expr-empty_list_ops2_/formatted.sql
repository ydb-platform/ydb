/* syntax version 1 */
/* postgres can not */
SELECT
    ListHasItems([]),
    Yql::Filter(
        [], ($x) -> {
            RETURN $x;
        }
    ),
    Yql::Map(
        [], ($x) -> {
            RETURN $x * 2;
        }
    ),
    Yql::FlatMap(
        [], ($x) -> {
            RETURN Just($x * 2);
        }
    ),
    Yql::OrderedFilter(
        [], ($x) -> {
            RETURN $x;
        }
    ),
    Yql::OrderedMap(
        [], ($x) -> {
            RETURN $x * 2;
        }
    ),
    Yql::OrderedFlatMap(
        [], ($x) -> {
            RETURN Just($x * 2);
        }
    ),
    ListFilter(
        [], ($x) -> {
            RETURN $x;
        }
    ),
    ListMap(
        [], ($x) -> {
            RETURN $x * 2;
        }
    ),
    ListFlatMap(
        [], ($x) -> {
            RETURN Just($x * 2);
        }
    ),
    ListCollect([]),
    ListSort([]),
    ListReverse([]),
    ListEnumerate([]),
    ListSkip([], 1),
    ListTake([], 1),
    ListIndexOf([], 1),
    ListUniq([]),
    ListAny([]),
    ListAll([]),
    ListHas([], 1),
    ListHead([]),
    ListLast([]),
    ListMin([]),
    ListMax([]),
    ListSum([]),
    ListAvg([]),
    ListHasItems(Just([])),
    ListFilter(
        Just([]), ($x) -> {
            RETURN $x;
        }
    ),
    ListMap(
        Just([]), ($x) -> {
            RETURN $x * 2;
        }
    ),
    ListFlatMap(
        Just([]), ($x) -> {
            RETURN Just($x * 2);
        }
    ),
    ListCollect(Just([])),
    ListSort(Just([])),
    ListReverse(Just([])),
    ListEnumerate(Just([])),
    ListSkip(Just([]), 1),
    ListTake(Just([]), 1),
    ListUniq(Just([])),
    ListAny(Just([])),
    ListAll(Just([])),
    ListHas(Just([]), 1),
    ListHead(Just([])),
    ListLast(Just([])),
    ListMin(Just([])),
    ListMax(Just([])),
    ListSum(Just([])),
    ListAvg(Just([])),
;
