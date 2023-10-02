/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;

$keys = ($key) -> {
    return AsTuple($key, $key);
};

select * from Input order by $keys(value);
