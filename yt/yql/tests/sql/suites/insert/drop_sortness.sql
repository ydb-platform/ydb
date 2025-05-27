/* postgres can not */
insert into plato.Output with truncate
SELECT
    value
FROM plato.Input;