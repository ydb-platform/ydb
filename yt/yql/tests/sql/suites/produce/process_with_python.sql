/* postgres can not */
/* syntax version 1 */
$udfScript = @@
def Dup(s):
    return [{"value":s},{"value":s}]
@@;

$udf = Python::Dup(Callable<(String)->List<Struct<value:String>>>, $udfScript);

process plato.Input0 using $udf(value);
