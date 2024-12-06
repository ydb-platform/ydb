PRAGMA BlockEngine = 'force';
USE plato;
$match = Re2::Match(@@\d+@@);
$grep = Re2::Grep('911');

SELECT
    key,
    String::EscapeC(value) AS ok1,
    $match(key) AS no_block_udf1,
    $grep(key) AS no_block_udf2,
    AsList(key) AS no_block_list,
    AsSet(key) AS no_block_set_and_void,
    CAST(key AS Double) AS no_block_cast,
    AsTuple(key, DyNumber("123")) AS no_block_dynumber,
FROM
    Input
;
