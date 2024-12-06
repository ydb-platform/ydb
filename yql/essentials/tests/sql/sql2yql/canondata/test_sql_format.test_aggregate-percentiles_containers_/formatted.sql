SELECT
    key,
    median(val) AS med,
    percentile(val, AsTuple(0.2, 0.4, 0.6)) AS ptuple,
    percentile(val, AsStruct(0.2 AS p20, 0.4 AS p40, 0.6 AS p60)) AS pstruct,
    percentile(val, AsList(0.2, 0.4, 0.6)) AS plist,
FROM (
    SELECT
        key,
        CAST(value AS int) AS val
    FROM plato.Input
)
GROUP BY
    key
ORDER BY
    key;

SELECT
    median(val) AS med,
    percentile(val, AsTuple(0.2, 0.4, 0.6)) AS ptuple,
    percentile(val, AsStruct(0.2 AS p20, 0.4 AS p40, 0.6 AS p60)) AS pstruct,
    percentile(val, AsList(0.2, 0.4, 0.6)) AS plist,
FROM (
    SELECT
        key,
        CAST(value AS int) AS val
    FROM plato.Input
);
