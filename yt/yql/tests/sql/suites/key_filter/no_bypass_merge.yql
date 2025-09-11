/* ignore runonopt plan diff - extra LogicalOptimizer-PushdownOpColumns */
pragma yt.PruneKeyFilterLambda = 'true';

USE plato;

$src = select * from Input where key == "1" || "5" || "0";
select key, subkey from $src;
select key, value from $src where key >= "000" and key < "999" and len(value) > 0;
