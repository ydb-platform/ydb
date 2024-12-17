USE plato;

SELECT
    key as key,
    "a" || min(subkey || "q") as x1,
    "b" || min(subkey || "q") as x2,
    "c" || max(value) as x3,
    "d" || max(value) as x4,
    "1" || min(distinct subkey) as y1,
    "2" || min(distinct subkey) as y2,
    "3" || max(distinct value) as y3,
    "4" || max(distinct value) as y4,
    percentile(x, 0.5),
    percentile(x, 0.9)
FROM (SELECT key, subkey, value, Length(key) as x from Input)
GROUP BY key
ORDER BY key;