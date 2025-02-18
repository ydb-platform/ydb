/* postgres can not */
SELECT
    if(LENGTH(value) > 2, "long", "short") AS if,
    if(false, 3) AS no_else
FROM plato.Input
;
