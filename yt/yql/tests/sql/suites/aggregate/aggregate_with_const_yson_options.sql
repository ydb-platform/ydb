USE plato;

SELECT
  key,
  Yson::SerializeJson(Yson::From(AGGREGATE_LIST(value), Yson::Options(true AS Strict))) as value
FROM Input
GROUP BY key
ORDER BY key
