/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('sum (Collect (Condense stream (Uint32 '0) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Add state item)))))
))@@;

INSERT INTO Output
REDUCE Input ON key USING $udf(cast(subkey as uint32) ?? 0) ASSUME ORDER BY key;
