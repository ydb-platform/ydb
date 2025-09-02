/* postgres can not */
/* kikimr can not - yt pragma */
USE plato;

PRAGMA yt.MaxExtraJobMemoryToFuseOperations="550m";
PRAGMA yt.CombineCoreLimit="128m";

$i = (SELECT * FROM Input WHERE key < "900");

SELECT key, sum(cast(subkey as Int32)) as s FROM $i GROUP BY key ORDER BY key, s;

SELECT key, some(subkey) as s FROM $i GROUP BY key ORDER by key, s;

SELECT key, some(value) as s FROM $i GROUP BY key ORDER BY key, s;
