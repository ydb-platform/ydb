/* postgres can not */
/* syntax version 1 */
use plato;
$udfScript = @@
def AppendNum(name, age):
    return name + str(age).encode('utf-8')
@@;

$udf = Python3::AppendNum(Callable<(String, Int32?)->String>, $udfScript);

select $udf(value, cast(subkey as Int32)) as val from Input;
