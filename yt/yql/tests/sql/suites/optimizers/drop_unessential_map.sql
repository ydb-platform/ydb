USE plato;

PRAGMA yt.EnableFuseMapToMapReduce;
PRAGMA warning("disable", "4510");

SELECT * FROM Input0 WHERE YQL::Unessential(value != "not present", true);
SELECT * FROM Input0 WHERE YQL::Unessential(value != "not present 1", true) AND YQL::Unessential(value != "not present 2", true);
SELECT * FROM Input0 WHERE YQL::Unessential(value != "not present 3", true) OR NOT (value != "not present 3");
SELECT * FROM Input0 WHERE YQL::Unessential(YQL::Unessential(value != "other not present 4", true) OR NOT (value != "other not present 4") OR value != "not present 4", true) OR NOT (value != "not present 4");

$mapreduce_output = SELECT key FROM Input0 WHERE YQL::Unessential(key != "not present 5", true) GROUP BY key LIMIT 1;
-- optimizer must not go beyond operation bounds
SELECT * FROM Input0 WHERE key || Unwrap($mapreduce_output) != "not present 5";
