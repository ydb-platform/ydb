/* postgres can not */
USE plato;

$max = select max(key) from Input;
$list = select key from Input where subkey > "1";

select * from (
    select if(key = $max, "max", key) as key, value from Input
)
where key in compact $list