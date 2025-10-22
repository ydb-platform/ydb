/* syntax version 1 */
/* postgres can not */
$udf = YQL::@@(lambda '(x) 
(FlatMap x 
   (lambda '(y) (AsList y y))
))@@;

process plato.Input0 using $udf(TableRows());
