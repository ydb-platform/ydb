USE plato;

SELECT
    min(key),
    min(CAST(key AS int8)),
    min(CAST(key AS uint16)),
    min(CAST(key AS int32)),
    min(1u / 0u),
    min(2),
    min(if(key == 10u, key)),
    min(if(key == 100u, key)),
    min(FALSE),
    min(key == 10u),
    min(key >= 10u),
    min(key == 20u),
    min(if(key == 10u, TRUE)),
    min(if(key == 100u, TRUE)),
    min(if(key >= 10u, TRUE)),
FROM Input;
