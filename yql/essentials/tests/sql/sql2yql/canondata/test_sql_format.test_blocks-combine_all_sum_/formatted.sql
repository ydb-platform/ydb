USE plato;

SELECT
    sum(key),
    sum(CAST(key AS int8)),
    sum(CAST(key AS uint16)),
    sum(CAST(key AS int32)),
    sum(1u / 0u),
    sum(2),
    sum(if(key == 10u, key)),
    sum(if(key == 100u, key))
FROM
    Input
;
