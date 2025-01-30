/* postgres can not */
/* kikimr can not - anon tables */
USE plato;

insert into @a
SELECT
    *
FROM Input0
ORDER BY key ASC, subkey ASC;

commit;

select * from @a
ORDER BY key ASC;
