USE plato;

SELECT
    max(key),
    max(CAST(key AS int8)),
    max(CAST(key AS uint16)),
    max(CAST(key AS int32)),
    max(1u / 0u),
    max(2),
    max(if(key == 10u, key)),
    max(if(key == 100u, key)),
    max(FALSE),
    max(key == 10u),
    max(key >= 10u),
    max(key == 20u),
    max(if(key == 10u, TRUE)),
    max(if(key == 100u, TRUE)),
    max(if(key >= 10u, TRUE)),
FROM Input;
