/* postgres can not */
$query = (
    SELECT
        AsTuple(count(*), min(value))
    FROM
        plato.Input

--WHERE 1=0
);

SELECT
    $query ?? AsTuple(0, "") AS cnt
;
