/* postgres can not */
use plato;
pragma DisableSimpleColumns;
discard select 1;
discard select * from Input;
discard select * from Input where key<"foo";
discard select * from Input as a join Input as b using(key);
discard select sum(length(value)), key, subkey from Input group by rollup(key,subkey) order by key, subkey;
discard select * from (select key || "a" || "b" as key from Input) as a join (select key || "ab" as key from Input) as b using(key); 
