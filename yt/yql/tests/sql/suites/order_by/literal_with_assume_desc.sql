/* postgres can not */
/* multirun can not */
/* hybridfile can not YQL-17743 */
/* syntax version 1 */
use plato;

insert into Output
select x
from (select ListFromRange(10,0,-1) as x)
flatten by x
assume order by x desc;
