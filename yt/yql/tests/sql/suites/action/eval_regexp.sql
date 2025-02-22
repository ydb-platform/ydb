/* syntax version 1 */
/* postgres can not */
USE plato;
$arg1 = "" || "";
$arg2 = (Unicode::ToUpper("i") ?? "") || "npu.";
$arg3 = "" || "";
$arg4 = "" || "raw";

SELECT
    count(*)
FROM REGEXP($arg1,$arg2,$arg3,$arg4);
