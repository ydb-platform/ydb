pragma BlockEngine='force';

USE plato;
$match = Re2::Match(@@\d+@@);
$grep = Re2::Grep('911');

SELECT
    key,
    String::EscapeC(value) as ok1,
    $match(key) as no_block_udf1,
    $grep(key) as no_block_udf2,
    AsList(key) as no_block_list,
    AsSet(key) as no_block_set_and_void,
    cast(key as Double) as no_block_cast,
    AsTuple(key, DyNumber("123")) as no_block_dynumber,
FROM Input;
