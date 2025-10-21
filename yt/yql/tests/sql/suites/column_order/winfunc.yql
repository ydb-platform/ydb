use plato;
pragma OrderedColumns; 

select 
    a.*
    , lag(key) over (order by subkey) as prev_k
    , min(key) over (order by subkey) as min_k
from Input as a
order by subkey
