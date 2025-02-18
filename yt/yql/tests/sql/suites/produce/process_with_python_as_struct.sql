/* postgres can not */
/* syntax version 1 */
$udfScript = @@
def Dup(s):
    return [s, s];
@@;

$udf = Python::Dup(Callable<(String)->List<String>>, $udfScript);

process plato.Input0 using $udf(value) as val;
