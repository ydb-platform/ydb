/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;
$udf = YQL::@@(lambda '(x) 
(FlatMap x 
   (lambda '(y) (Just (AsStruct '('key (Concat (String '"0") (Member y 'key))) '('subkey (Member y 'subkey)) '('value (Member y 'value)))))
))@@;

INSERT INTO Output WITH truncate
PROCESS plato.Input
USING $udf(TableRows())
ASSUME ORDER BY
    key;
