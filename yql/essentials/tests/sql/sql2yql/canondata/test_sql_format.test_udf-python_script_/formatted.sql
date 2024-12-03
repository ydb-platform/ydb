/* postgres can not */
/* syntax version 1 */
USE plato;
$udfScript = @@
def AppendNum(name, age):
    return name + str(age).encode('utf-8')
@@;
$udf = Python3::AppendNum(Callable<(String, Int32?) -> String>, $udfScript);

SELECT
    $udf(value, CAST(subkey AS Int32)) AS val
FROM Input;
