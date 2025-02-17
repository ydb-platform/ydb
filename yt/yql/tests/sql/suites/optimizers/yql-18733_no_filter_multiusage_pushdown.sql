pragma config.flags("OptimizerFlags", "FilterPushdownEnableMultiusage");
USE plato;

$src = select distinct key from Input where value = 'ddd';

select * from Input where key = $src;
