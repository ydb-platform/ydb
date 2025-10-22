/* postgres can not */
/* multirun can not */
/* syntax version 1 */
use plato;

$udf = YQL::@@(lambda '(x) 
(FlatMap x 
   (lambda '(y) (Just (AsStruct '('key (Concat (String '"0") (Member y 'key))) '('subkey (Member y 'subkey)) '('value (Member y 'value)))))
))@@;

insert into Output with truncate
process plato.Input using $udf(TableRows()) assume order by key;
