USE plato;

SELECT sum(count) from (
select count(*) as count from Input WHERE key < "100"
);
