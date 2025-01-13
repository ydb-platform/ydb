/* syntax version 1 */
/* postgres can not */
$foo = ($x) -> {
    RETURN AsTuple($x, $x);
};

$ids = (
    SELECT
        AGGREGATE_LIST(id)
    FROM (
        SELECT
            '1' AS id
        UNION ALL
        SELECT
            '2' AS id
    )
);

$first_ids, $second_ids = $foo(unwrap($ids));

SELECT
    $first_ids AS one,
    $second_ids AS two
;
