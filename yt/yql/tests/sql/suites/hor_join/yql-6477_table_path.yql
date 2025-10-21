/* postgres can not */
/* syntax version 1 */
/* kikimr can not - yt pragma */
USE plato;

pragma yt.MaxExtraJobMemoryToFuseOperations="512M";

$udfScript = @@
def AsIs(s):
    return s
@@;

$udf = Python::AsIs(Callable<(String)->String>, $udfScript);

select TablePath() as path from Input
union all
select TablePath() as path from Input where $udf(key) > "080"
;
