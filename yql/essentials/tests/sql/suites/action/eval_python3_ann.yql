/* syntax version 1 */
/* postgres can not */
$script = @@ 
from yql.typing import *

def f(x:Int32, y:Int32)->Int32:
    """
    a simple sum UDF
    """
    return x + y
@@;

--$f = Python3::f(EvaluateType(ParseTypeHandle(Core::PythonFuncSignature(AsAtom("Python3"), $script, "f"))), $script);
$f = Python3::f($script);

select $f(1, 2);
