USE plato;

SELECT
    avg(key),
    avg(cast(key as int8)),
    avg(cast(key as uint16)),
    avg(cast(key as int32)),
    avg(1u/0u),
    avg(2),
    avg(if(key=10u,key)),
    avg(if(key=100u,key)),
    avg(key=10u)
FROM Input