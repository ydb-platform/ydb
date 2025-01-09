pragma SimpleColumns;
use plato;

$q = (select CAST(key as Int32) as key, CAST(subkey as Int32) as subkey, value from Input);

select t.*, sum(subkey) over w as subkey_sum, sum(key) over w from $q as t window w as (partition by key order by value) order by key,subkey;
