/* custom error:Failed to cast arguments*/
/* dqfile can not */
USE plato;

$f=Python3::f(@@
def f(x):
    """
    Callable<(Int32)->Int32>
    """
    return ""
@@);

select $f(0);
