$s = @@
def f(x):
   return x + 1

def g(x):
   return x * 2
@@;

$l = LinearType(Int32);
$p = Python::f(Callable<(Int32)->$l>, $s);
$c = Python::g(Callable<($l)->Int32>, $s);

select $c($p(1));


