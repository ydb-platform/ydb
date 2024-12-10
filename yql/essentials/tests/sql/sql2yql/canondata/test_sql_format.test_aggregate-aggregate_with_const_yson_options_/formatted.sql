USE plato;

SELECT
    key,
    Yson::SerializeJson(Yson::From(AGGREGATE_LIST(value), Yson::Options(TRUE AS Strict))) AS value
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
