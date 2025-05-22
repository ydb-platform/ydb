/* postgres can not */
USE plato;

$rc = (select count(*) from Input);
$sample_size = 10;

select * from Input tablesample bernoulli(MIN_OF($sample_size * 100.0 / $rc, 100.0)) order by key;
