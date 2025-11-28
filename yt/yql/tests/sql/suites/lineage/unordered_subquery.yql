USE plato;

define subquery $f() as
    SELECT * FROM Input
end define;

insert into Output with truncate
    select * from $f()

