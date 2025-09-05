/* postgres can not */
/* syntax version 1 */
USE plato;

$script = @@
def f(s):
  return int(s)
@@;

$callable = Python::f(Callable<(String)->Uint64?>,$script);

$i = unwrap($callable("2"));

SELECT
    key,
    SOME(value) as value
FROM Input
GROUP BY key
ORDER BY key LIMIT $i;
