/* postgres can not */
USE plato;

$predicate = ($x) -> {
    RETURN $x == "911"
};
$field = "field";

SELECT *
FROM Input
WHERE $predicate($field);
