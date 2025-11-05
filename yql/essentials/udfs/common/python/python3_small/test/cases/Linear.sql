$s = @@
def f(x):
   return x + 1

def g(x):
   return x * 2
@@;

$p = Python::f(Callable<(Int32)->Linear<Int32>>, $s);
$c = Python::g(Callable<(Linear<Int32>)->Int32>, $s);

select $c($p(1));


