/* postgres can not */
/* syntax version 1 */
/* kikimr can not - range not supported */
$script = @@
def f(f):
  def ft(s):
    return True
  def ff(s):
    return False
  return f and ft or ff
@@;

$callable = Python3::f(Callable<(Bool)->Callable<(String)->Bool>>,$script);
$callableT = $callable(Re2::Match('test.*')('testfets'));

select count(*) as count from plato.filter(``, $callableT);
