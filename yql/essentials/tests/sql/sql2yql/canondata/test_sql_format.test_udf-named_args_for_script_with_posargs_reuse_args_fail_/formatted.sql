/* custom error:Argument with name age was already used for positional argument #2*/
USE plato;

$udfScript = @@
def AppendInfo(a_name, a_age = None, a_region = None):
    res = a_name.decode('utf-8')
    if a_age:
        res += ", age: " + repr(a_age)
    if a_region:
        res += ", region: " + repr(a_region)
    return res.encode('utf-8')
@@;

$udf = Python3::AppendInfo(Callable<(name: String, [age: Int32?, region: Int32?]) -> String>, $udfScript);

$data = (
    SELECT
        CAST(key AS int32) AS age,
        CAST(subkey AS int32) AS region,
        value AS name
    FROM
        Input
);

SELECT
    $udf(name, age, age AS age) AS val
FROM
    $data
; -- age is reused as named after positional
