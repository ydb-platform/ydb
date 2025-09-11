/* postgres can not */
use plato;

$foo = (
  select key, subkey, value,
    sum(cast(subkey as uint32)) over w as sks
  from Input
  window w as (partition by key order by subkey)
);

$bar = (
  select key, subkey,
    sum(cast(subkey as uint32)) over w as sks, 
    avg(cast(subkey as uint32)) over w as ska
  from Input4
  window w as (partition by key order by subkey)
);
       
select key,subkey, value from $foo order by key, subkey;
select key,ska from $bar order by key,ska;

