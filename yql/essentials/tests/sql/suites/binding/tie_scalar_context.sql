/* syntax version 1 */
/* postgres can not */

$foo = ($x) -> {
    return AsTuple($x, $x);
};

$ids = (
    SELECT AGGREGATE_LIST(id) 
    FROM (
        select "1" as id union all select "2" as id
    )
);


$first_ids, $second_ids = $foo(unwrap($ids));
select $first_ids as one, $second_ids as two;

