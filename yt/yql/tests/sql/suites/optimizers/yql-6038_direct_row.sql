/* syntax version 1 */
/* postgres can not */
USE plato;

$queries_0 = (select distinct key from Input);

$queries = (select TableRecordIndex() as j, key from $queries_0);

$count = (select count(*) from $queries);

$users_0 = (
  select ListFromRange(0, 3) as lst, TableRecordIndex() as idx, subkey from Input as t
);

$users = (
    select
        cast(Random(idx + x) as Uint64) % $count as j,
        subkey
    from $users_0
    flatten by lst as x
);

select *
from $queries as queries join $users as users using(j)
order by key, subkey;
