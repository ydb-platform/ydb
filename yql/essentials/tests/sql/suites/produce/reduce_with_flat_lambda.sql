/* syntax version 1 */
/* kikimr can not */
USE plato;

$udf_stream = ($input)->{ return $input };

$res = REDUCE Input0 ON key using all $udf_stream(TableRows());

select * from $res order by value;
