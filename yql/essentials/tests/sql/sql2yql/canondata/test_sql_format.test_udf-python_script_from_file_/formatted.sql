/* postgres can not */
/* syntax version 1 */
USE plato;
$udfScript = FileContent("python_script.py");
$udf = Python::AppendNum(Callable<(String, Int32?) -> String>, $udfScript);

SELECT
    $udf(value, CAST(subkey AS Int32)) AS value
FROM
    Input
;
