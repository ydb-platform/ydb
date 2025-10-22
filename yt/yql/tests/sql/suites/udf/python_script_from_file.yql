/* postgres can not */
/* syntax version 1 */
use plato;
$udfScript = FileContent("python_script.py");
$udf = Python::AppendNum(Callable<(String, Int32?)->String>, $udfScript);

select $udf(value, cast(subkey as Int32)) as value from Input;
