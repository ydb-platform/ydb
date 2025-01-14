/* postgres can not */
insert into plato.Output with truncate
SELECT
    key,value
FROM plato.Input;