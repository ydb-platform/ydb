/* postgres can not */
USE plato;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Uint32 '0) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Add state item)))))
))@@;

--INSERT INTO Output
$res = (REDUCE (select AsList(key) as key, value from Input1) ON key USING $udf(cast(value as uint32) ?? 0));

select * from $res order by key;
