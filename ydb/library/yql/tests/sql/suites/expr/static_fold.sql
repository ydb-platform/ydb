/* postgres can not */
/* syntax version 1 */

$st_many = <|a: "one", b: "two", c: "three", d: "four"|>;
$st_single = <|a: "zero"|>;
$st_empty = <||>;

$concat = ($left, $right) -> { return $left || $right; };
$start = ($value) -> { return "(" || $value || ")"; };

SELECT
    StaticFold($st_many, "->", $concat),
    StaticFold($st_single, "->", $concat),
    StaticFold($st_empty, "->", $concat),
    StaticFold1($st_many, $start, $concat),
    StaticFold1($st_single, $start, $concat),
    StaticFold1($st_empty, $start, $concat);