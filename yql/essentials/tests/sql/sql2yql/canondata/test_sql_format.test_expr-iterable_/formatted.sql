/* postgres can not */
/* syntax version 1 */
$a = Yql::ToList(() -> (Yql::Iterator([1, 2, 3])));

SELECT
    ListExtend($a, $a),
    ListHasItems($a),
    ListLength($a)
;

$b = Yql::ToList(() -> (Yql::EmptyIterator(Stream<Int32>)));

SELECT
    ListExtend($b, $b),
    ListHasItems($b),
    ListLength($b)
;

$c = Yql::ToList(() -> (Yql::EmptyIterator(Stream<Int32>)));

SELECT
    ListExtend($b, $b),
    ListLength($c),
    ListHasItems($c)
;

SELECT
    ListMap(
        ListFromRange(1, 4), ($x) -> {
            $y = Yql::ToList(() -> (Yql::Iterator([1, 2, $x])));
            RETURN ListExtend($y, $y);
        }
    )
;
