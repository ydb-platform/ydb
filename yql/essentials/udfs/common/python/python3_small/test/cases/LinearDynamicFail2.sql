$s = @@
def f(x):
    class Once:
        def __init__(self, v):
            self.v = v
            self.extracted = False

        def extract(self):
            assert not self.extracted
            self.extracted = True
            ret = self.v
            self.v = None
            return ret

    return Once(x + 1)

def g(x,n):
    return x.extract() + n
@@;

$p = Python::f(Callable<(Int32)->DynamicLinear<Int32>>, $s);
$c = Python::g(Callable<(DynamicLinear<Int32>,Int32)->Int32>, $s);

$a = $p(1);
select $c($a,0),$c($a,1);


