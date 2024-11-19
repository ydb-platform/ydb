/* postgres can not */
use plato;

select * from Input4 where subkey not in (select key || "0" from Input4) order by key,subkey; 

select * from Input4 where subkey in compact(select key || "0" from Input4) order by key,subkey;
