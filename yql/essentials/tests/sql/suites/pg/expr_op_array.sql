--!syntax_pg
select 1 = any(array[1,2]), 
       1 = any(array[2,3]), 
       2 = all(array[2,2]), 
       2 = all(array[2,3]),
       1 = any(array[null,1]),
       1 = any(array[null,2]),
       1 = all(array[null,1]),
       1 = any(null::_int4),
       1 = all(null::_int4),
       null = any(array[1,2]),
       null = all(array[1,2]),
       null = any(null::_int4),
       null = all(null::_int4);


