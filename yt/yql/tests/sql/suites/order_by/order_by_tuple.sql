/* postgres can not */
USE plato;

select * from (
    select key, AsTuple(key, subkey) as tpl from Input
) order by tpl;
