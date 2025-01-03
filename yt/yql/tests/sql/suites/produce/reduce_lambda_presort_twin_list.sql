/* postgres can not */
USE plato;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('superstring (Collect (Condense stream (String '"") (lambda '(item state) (Bool 'False)) (lambda '(item state)
    (Concat state (Concat (Member item 'char) (Member item 'num)))
  ))))
))@@;

--INSERT INTO Output
$res = (REDUCE Input1 PRESORT AsList(subkey), value desc ON key USING $udf(AsStruct(subkey as char, value as num)));

select * from $res order by key;
