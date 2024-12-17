/* postgres can not */
/* syntax version 1 */
$single = (
    SELECT
        key
    FROM
        plato.Input
);

$all = (
    SELECT
        *
    FROM
        $single
    ORDER BY
        key
    LIMIT 100
);

SELECT
    $all
;
