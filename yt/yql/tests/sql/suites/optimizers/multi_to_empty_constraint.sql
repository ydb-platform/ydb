/* syntax version 1 */

USE plato;

select key, some(subkey) from (select * from Input where key > "010" and value in []) group by key
union all
select key, some(subkey) from (select * from Input where key > "020" and value in []) group by key
;
