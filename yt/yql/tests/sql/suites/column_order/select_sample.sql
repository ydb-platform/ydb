/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

select * from Input tablesample bernoulli(100.0);

