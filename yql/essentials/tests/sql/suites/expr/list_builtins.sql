/* postgres can not */
/* syntax version 1 */
$list = AsList(3, 1, 2, 3);
$other_list = AsList(4);
$bool_list = AsList(true, false, true);
$struct_list = AsList(
    AsStruct(1 AS one, 2 AS two),
    AsStruct(-1 AS one, -2 AS two)
);

$script = @@
def formula(a, b, c, d):
    return a * b + c // d
@@;
$udf = Python3::formula(
    Callable<(Int64, Int64, Int64, Int64)->Int64>,
    $script
);

$lambdaSum = ($x, $y) -> { RETURN $x + $y; };
$lambdaMult = ($x) -> { RETURN 4 * $x; };
$lambdaTuple = ($i, $s) -> { RETURN ($i * $s, $i + $s); };
$lambdaInc = ($i) -> { RETURN ($i + 1, $i + 2); };

SELECT
    ListLength($list) AS length,
    ListExtend($list, $other_list) AS extend,
    ListZip($list, $other_list) AS zip,
    ListZipAll($list, $other_list) AS zipall,
    ListEnumerate($list) AS enumerate,
    ListReverse($list) AS reverse,
    ListSkip($list, 2) AS skip,
    ListTake($list, 2) AS take,
    ListSort($list) AS sort,
    ListSort($struct_list, ($x) -> { return $x.two; }) AS sort_structs,
    ListMap($list, ($item) -> { return $udf($item, 6, 4, 2); }) AS map,
    ListFlatMap($list, ($item) -> { return $item / 0; }) AS flatmap,
    ListFilter($list, ($item) -> { return $item < 3; }) AS filter,
    ListAny($bool_list) AS any,
    ListAll($bool_list) AS all,
    ListMax($list) AS max,
    ListMin($list) AS min,
    ListSum($list) AS sum,
    ListAvg($list) AS avg,
    ListUniq($list) AS uniq,
    ListConcat(ListMap($list, ($item) -> { return CAST($item AS String); })) AS concat,
    ListExtract($struct_list, "two") AS extract,
    ListMap($list, ($item) -> { return CAST($item AS Double);}),
    ListCreate(Tuple<Int64,Double>),
    ListCreate(TypeOf("foo")),
    ListFold($list, 6, $lambdaSum),
    ListFold([], 3, $lambdaSum),
    ListFold(Just($list), 6, $lambdaSum),
    ListFold(Just([]), 3, $lambdaSum),
    ListFold(Null, 3, $lambdaSum),
    ListFold1($list, $lambdaMult, $lambdaSum),
    ListFold1([], $lambdaMult, $lambdaSum),
    ListFold1(Just($list), $lambdaMult, $lambdaSum),
    ListFold1(Just([]), $lambdaMult, $lambdaSum),
    ListFold1(Null, $lambdaMult, $lambdaSum),
    ListFoldMap($list, 1, $lambdaTuple),
    ListFoldMap([], 1, $lambdaTuple),
    ListFoldMap(Just($list), 1, $lambdaTuple),
    ListFoldMap(Just([]), 1, $lambdaTuple),
    ListFoldMap(Null, 1, $lambdaTuple),
    ListFold1Map($list, $lambdaInc, $lambdaTuple),
    ListFold1Map([], $lambdaInc, $lambdaTuple),
    ListFold1Map(Just($list), $lambdaInc, $lambdaTuple),
    ListFold1Map(Just([]), $lambdaInc, $lambdaTuple),
    ListFold1Map(Null, $lambdaInc, $lambdaTuple);
