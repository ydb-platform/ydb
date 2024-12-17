--sanitizer ignore memory
$script = @@
def f(string, uuid, yson):
    return (string, str(type(string)), uuid, str(type(uuid)), yson, str(type(yson)))

f._yql_bytes_decode_mode = 'strict'
@@;

$udf = Python3::f(Callable<(String?, UUid?, Yson?)->Tuple<String?, String, UUid?, String, Yson?, String>>, $script);

SELECT $udf("string", UUid('1812bc18-5838-4cde-98aa-287302697b90'), cast(@@{"abc"=1}@@ as yson));
