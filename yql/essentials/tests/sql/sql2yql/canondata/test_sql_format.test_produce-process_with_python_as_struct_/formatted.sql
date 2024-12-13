/* postgres can not */
/* syntax version 1 */
$udfScript = @@
def Dup(s):
    return [s, s];
@@;

$udf = Python::Dup(Callable<(String) -> List<String>>, $udfScript);

PROCESS plato.Input0
USING $udf(value) AS val;
