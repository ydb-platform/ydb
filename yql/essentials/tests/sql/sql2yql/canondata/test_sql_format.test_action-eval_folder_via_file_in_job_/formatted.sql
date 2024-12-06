/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.FolderInlineItemsLimit = "0";
$script = @@
def f(s):
  return True
@@;
$callable = Python3::f(Callable<(String) -> Bool>, $script);

$list = (
    SELECT
        aggregate_list(Path)
    FROM (
        SELECT
            Path
        FROM
            folder("")
        WHERE
            Type == "table" AND $callable(Path)
        LIMIT 30
    )
);

SELECT
    count(*)
FROM
    each($list)
;
