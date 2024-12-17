/* postgres can not */
/* syntax version 1 */
/* kikimr can not - range not supported */
SELECT
    count(*) AS count
FROM
    plato.filter(``, Unicode::IsUtf)
;

$script = @@
def f(s):
  return True
@@;

$callable = Python3::f(Callable<(String) -> Bool?>, $script);

SELECT
    count(*) AS count
FROM
    plato.filter(``, $callable)
;
