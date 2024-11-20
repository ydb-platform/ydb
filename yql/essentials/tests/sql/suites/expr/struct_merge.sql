/* syntax version 1 */

$merge = ($_name, $l, $r) -> { return Coalesce($l, 0) + Coalesce($r, 0); };
$left = <|a: 1, b: 2, c: 3|>;
$right = <|c: 1, d: 2, e: 3|>;

SELECT
    StructUnion($left, $right),
    StructUnion($left, $right, $merge),
    StructIntersection($left, $right),
    StructIntersection($left, $right, $merge),
    StructDifference($left, $right),
    StructSymmetricDifference($left, $right)
;
