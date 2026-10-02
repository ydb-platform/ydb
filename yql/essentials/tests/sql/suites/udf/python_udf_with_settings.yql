$f = Python::f(Callable<(String,y:String)->String>,@@
def f(x,y):
    return x + y
@@);

$f = Udf($f, "64.0" as Cpu, "4294967296" as ExtraMem);

SELECT
    $f('foo', '?'),
    $f('foo', '!' as y)
