USE plato;

pragma yt.MapJoinLimit="1M";

$t = (
SELECT 
    l.key as key,
    r.subkey as subkey,
    l.value || r.value as value
FROM 
    Input1 as l
CROSS JOIN 
    Input2 as r
);

SELECT
    l.*,
    r.value as rvalue
FROM 
    $t as l 
LEFT JOIN 
    Input3 as r 
ON l.key = coalesce("" || r.key, "") 
