$s = @@
def f(x):
   return x + 1
@@;

$f = Python::f(Callable<(Tagged<Int32,'x'>)->Tagged<Int32,'y'>>, $s);

select $f(AsTagged(1,'x'));


