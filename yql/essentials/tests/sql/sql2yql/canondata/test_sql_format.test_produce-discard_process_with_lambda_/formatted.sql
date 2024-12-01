/* syntax version 1 */
/* postgres can not */
$udf = YQL::@@(lambda '(x) 
(FlatMap x 
   (lambda '(y) (AsList y y))
))@@;

DISCARD PROCESS plato.Input0
USING $udf(TableRows());
