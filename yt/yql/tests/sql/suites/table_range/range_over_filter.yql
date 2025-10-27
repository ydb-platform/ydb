/* postgres can not */
/* syntax version 1 */
/* kikimr can not - range not supported */
select count(*) as count from plato.filter(``, Unicode::IsUtf);

$script = @@
def f(s):
  return True
@@;

$callable = Python3::f(Callable<(String)->Bool?>,$script);
select count(*) as count from plato.filter(``, $callable);
