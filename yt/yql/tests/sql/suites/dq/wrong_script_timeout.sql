/* dqfile can not */
USE plato;

$f=Python3::f(@@
def f(x):
    """
    Callable<(Int32)->Int32>
    """
    import time
    time.sleep(60)
    return 0
@@);

select $f(0);
