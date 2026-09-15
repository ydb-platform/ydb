USE plato;

SELECT
    min(key),
    min(cast(key as int8)),
    min(cast(key as uint16)),
    min(cast(key as int32)),
    min(1u/0u),
    min(2),
    min(if(key=10u,key)),
    min(if(key=100u,key)),
    min(false),
    min(key=10u),
    min(key>=10u),
    min(key=20u),
    min(if(key=10u,true)),
    min(if(key=100u,true)),
    min(if(key>=10u,true)),
FROM Input