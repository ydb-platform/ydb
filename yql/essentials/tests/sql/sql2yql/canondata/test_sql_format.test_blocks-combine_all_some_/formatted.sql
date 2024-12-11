USE plato;

SELECT
    some(key),
    some(CAST(key AS int8)),
    some(CAST(key AS uint16)),
    some(CAST(key AS int32)),
    some(1u / 0u),
    some(2),
    some(if(key == 10u, key)),
    some(if(key == 100u, key)),
    some(FALSE),
    some(key == 10u),
    some(key >= 10u),
    some(key == 20u),
    some(if(key == 10u, TRUE)),
    some(if(key == 100u, TRUE)),
    some(if(key >= 10u, TRUE)),
FROM
    Input
;
