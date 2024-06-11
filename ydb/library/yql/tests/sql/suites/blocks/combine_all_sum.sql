USE plato;

SELECT
    sum(key),
    sum(cast(key as int8)),
    sum(cast(key as uint16)),
    sum(cast(key as int32)),
    sum(1u/0u),
    sum(2),
    sum(if(key=10u,key)),
    sum(if(key=100u,key))
FROM Input