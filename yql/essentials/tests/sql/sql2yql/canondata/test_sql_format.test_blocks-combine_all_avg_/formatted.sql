USE plato;

SELECT
    avg(key),
    avg(CAST(key AS int8)),
    avg(CAST(key AS uint16)),
    avg(CAST(key AS int32)),
    avg(1u / 0u),
    avg(2),
    avg(if(key == 10u, key)),
    avg(if(key == 100u, key)),
    avg(key == 10u)
FROM Input;
