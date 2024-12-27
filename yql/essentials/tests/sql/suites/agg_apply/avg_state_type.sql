/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

select 
formattype(typeof(avg(null))),
formattype(typeof(avg(1))),
formattype(typeof(avg(decimal("10",10,1)))),
formattype(typeof(avg(interval("P10D")))),
formattype(typeof(avg(just(1)))),
formattype(typeof(avg(just(decimal("10",10,1))))),
formattype(typeof(avg(just(interval("P10D")))))
from (select 1) group by () with combine