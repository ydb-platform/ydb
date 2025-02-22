use plato;

select row_number() over (order by key) as rn, key from Input group by rollup(key, subkey) order by rn;
