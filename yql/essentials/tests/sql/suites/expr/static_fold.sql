/* syntax version 1 */

$st_many = <|a: "one", b: "two", c: "three", d: "four"|>;
$st_single = <|a: "zero"|>;
$st_empty = <||>;
$tup = AsTuple("first", "second");

$concat = ($item, $state) -> { return $state || $item; };
$start = ($value) -> { return "(" || $value || ")"; };

SELECT
    StaticFold($st_many, "->", $concat),
    StaticFold($st_single, "->", $concat),
    CAST(StaticFold($st_empty, "->", $concat) AS Optional<String>),
    StaticFold($tup, "->", $concat);

SELECT
    StaticFold1($st_many, $start, $concat),
    StaticFold1($st_single, $start, $concat),
    CAST(StaticFold1($st_empty, $start, $concat) AS Optional<String>),
    StaticFold1($tup, $start, $concat);