use plato;

define action $f($i) as 
    insert into Output
    select $i;
end define;

evaluate parallel for $i in [1,2,3] do $f($i);

