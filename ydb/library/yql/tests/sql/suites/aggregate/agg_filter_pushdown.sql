/* syntax version 1 */
USE plato;

select * from (
    select key, subkey, max(value) from Input group by key, subkey
    having count(*) < 100 and subkey > "0"
)
where key > "1" and Likely(subkey < "4")
order by key, subkey;
