USE plato;

select
    key,
    ob1 ?? b1,
    ob1 ?? false,
    ob1 ?? (1/0 > 0),
    (1/2 > 0) ?? ob1,
    (1/2 == 0) ?? b1,
    (key/0 >= 0) ?? true, 
    (key/0 >= 0) ?? b1,
    (key/2 >= 0) ?? false,
    
from Input

order by key;
