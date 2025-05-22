$f = Python::f(
    Callable<(String, y: String) -> String>, @@
def f(x,y):
    return x + y
@@
);

$f = Udf($f, '64.0' AS Cpu, '4294967296' AS ExtraMem);

SELECT
    $f('foo', '?'),
    $f('foo', '!' AS y)
;
