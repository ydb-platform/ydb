/* kikimr can not - no refs */
/* postgres can not */
USE plato;
pragma ResultRowsLimit="2";

SELECT key||"a" as key FROM Input order by key;
SELECT key||"b" as key FROM Input order by key;
