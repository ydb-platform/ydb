USE plato;

SELECT
    some(key),
    some(cast(key as int8)),
    some(cast(key as uint16)),
    some(cast(key as int32)),
    some(1u/0u),
    some(2),
    some(if(key=10u,key)),
    some(if(key=100u,key)),
    some(false),
    some(key=10u),
    some(key>=10u),
    some(key=20u),
    some(if(key=10u,true)),
    some(if(key=100u,true)),
    some(if(key>=10u,true)),
FROM Input