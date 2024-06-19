USE plato;

SELECT
    max(key),
    max(cast(key as int8)),
    max(cast(key as uint16)),
    max(cast(key as int32)),
    max(1u/0u),
    max(2),
    max(if(key=10u,key)),
    max(if(key=100u,key)),
    max(false),
    max(key=10u),
    max(key>=10u),
    max(key=20u),
    max(if(key=10u,true)),
    max(if(key=100u,true)),
    max(if(key>=10u,true)),
FROM Input