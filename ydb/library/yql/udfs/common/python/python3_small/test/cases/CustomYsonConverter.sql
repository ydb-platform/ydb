--sanitizer ignore memory
/* syntax version 1 */
$script = @@
import json

def yloads(z):
  return json.loads(str(z, 'latin-1').replace("=",":"))

def ydumps(z):
  return bytes(json.dumps(z).replace(":","="), 'latin-1')

def f(s):
    return (s.get("abc",0),s)

f._yql_convert_yson = (yloads,ydumps)
@@;

$udf = Python3::f(Callable<(Yson?)->Tuple<Int64, Yson?>>, $script);

SELECT $udf(cast(@@{"abc"=1}@@ as yson));
