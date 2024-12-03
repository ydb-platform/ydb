/* postgres can not */
/* syntax version 1 */
/* kikimr can not - yt pragma */
USE plato;
PRAGMA yt.MaxExtraJobMemoryToFuseOperations = "512M";
$udfScript = @@
def AsIs(s):
    return s
@@;
$udf = Python::AsIs(Callable<(String) -> String>, $udfScript);

SELECT
    TablePath() AS path
FROM Input
UNION ALL
SELECT
    TablePath() AS path
FROM Input
WHERE $udf(key) > "080";
