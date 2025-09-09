/* postgres can not */
/* syntax version 1 */
USE plato;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Nothing (OptionalType (DataType 'String))) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Coalesce state (Just item))))))
))@@;

$in = (SELECT * FROM Input ASSUME ORDER BY key, subkey);

$res = (REDUCE $in ON key USING $udf(value));

SELECT * FROM $res ORDER BY key;
