/* postgres can not */
/* kikimr can not - no refs */
USE plato;
pragma ResultSizeLimit="25";

SELECT key||"a" as key FROM Input order by key;
SELECT key||"b" as key FROM Input order by key;
