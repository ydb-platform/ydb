/* postgres can not */
/* hybridfile can not YQL-17743 */
use plato;

$list = AsList(AsStruct(1 as a, "2" as b, "3" as c), AsStruct(4 as a, "5" as b, "6" as c));

insert into Output
select * from as_table($list)
order by a desc, b, c desc;
