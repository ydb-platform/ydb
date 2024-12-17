/* syntax version 1 */
/* postgres can not */
$udf = YQL::@@(lambda '(x) 
(FlatMap x 
   (lambda '(y) (AsList y y))
))@@;

PROCESS plato.Input0
USING $udf(TableRows());
