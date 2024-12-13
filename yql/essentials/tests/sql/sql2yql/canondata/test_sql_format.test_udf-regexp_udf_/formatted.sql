/* postgres can not */
$regexp = Pire::Match("q.*");

SELECT
    *
FROM
    plato.Input
WHERE
    $regexp(value)
;
