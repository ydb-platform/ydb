/* postgres can not */
/* syntax version 1 */
use plato;
$udfScript = @@
def AppendInfo(a_name, a_age = None, a_region = None):
    res = a_name.decode('utf-8')
    if a_age:
        res += ", age: " + repr(a_age)
    if a_region:
        res += ", region: " + repr(a_region)
    return res.encode('utf-8')
@@;

$udf = Python3::AppendInfo(Callable<(name:String, [age:Int32?, region:Int32?])->String>, $udfScript);

$data = (select cast(key as int32) as age, cast(subkey as int32) as region, value as name from Input);

select $udf(name, age, age as age) as val from $data; -- age is reused as named after positional
