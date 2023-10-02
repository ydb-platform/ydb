/* postgres can not */
$query = (
select AsTuple(count(*),min(value)) FROM plato.Input
--WHERE 1=0
);
select $query ?? AsTuple(0,"") as cnt;
