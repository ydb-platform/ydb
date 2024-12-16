SELECT
    key,
    value
FROM
    plato.Input
ORDER BY
    key,
    String::SplitToList(value, "$", 2 AS Limit)[0]
;
