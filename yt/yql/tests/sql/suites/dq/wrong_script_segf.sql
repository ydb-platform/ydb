/* custom error:PrintBacktraceToStderr*/
/* dqfile can not */
USE plato;

$f=Python3::f(@@
def f(x):
    """
    Callable<(Int32)->Int32>
    """
    import ctypes
    def deref(addr, typ):
        return ctypes.cast(addr, ctypes.POINTER(typ)).contents
    print(deref(1, ctypes.c_int))
    return 0
@@);

select $f(0);
