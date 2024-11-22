/* syntax version 1 */
/* postgres can not */
use plato;
pragma yt.EvaluationTableSizeLimit="1";
select * from Input limit 1;
$tables = (select aggregate_list(Path) as dates from folder("") where Path like "Input%");
select count(*) from each($tables);
