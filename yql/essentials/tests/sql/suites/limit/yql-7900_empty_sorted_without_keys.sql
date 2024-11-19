/* postgres can not */
USE plato;

$cnt = (select count(*) from Input);
$offset = ($cnt + 10) ?? 0;

$data_limited = (select * from Input order by key || value limit 1 offset $offset);

$result_top = (SELECT subkey, Length(key) as l, key FROM $data_limited);

SELECT * FROM $result_top;
