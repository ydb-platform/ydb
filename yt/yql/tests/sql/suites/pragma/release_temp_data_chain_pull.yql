/* kikimr can not */
USE plato;

PRAGMA yt.ReleaseTempData="immediate";
PRAGMA yt.InflightTempTablesLimit="6";

DEFINE ACTION $action($param) AS

$key = CAST($param as String);

$a = "Input";
$b = (SELECT key,value FROM $a WHERE key != ($key || ".1") GROUP BY key, value ORDER BY value LIMIT 10);
$c = (SELECT key,value FROM $b WHERE key != ($key || ".2") GROUP BY key, value ORDER BY value LIMIT 10);
$d = (SELECT key,value FROM $c WHERE key != ($key || ".3") GROUP BY key, value ORDER BY value LIMIT 10);
$e = (SELECT key,value FROM $d WHERE key != ($key || ".4") GROUP BY key, value ORDER BY value LIMIT 10);
$f = (SELECT key,value FROM $e WHERE key != ($key || ".5") GROUP BY key, value ORDER BY value LIMIT 10);
$g = (SELECT key,value FROM $f WHERE key != ($key || ".6") GROUP BY key, value ORDER BY value LIMIT 10);

PROCESS $g;

END DEFINE;

EVALUATE FOR $param IN ListFromRange(1, 3) DO $action($param);
