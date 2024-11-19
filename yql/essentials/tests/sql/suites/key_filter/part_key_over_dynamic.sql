USE plato;

select * from (SELECT key from Input) as a where key>="030" and  key<="200";
