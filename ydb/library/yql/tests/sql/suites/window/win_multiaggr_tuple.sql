/* postgres can not */
use plato;

select MULTI_AGGREGATE_BY(AsTuple(subkey,value), AGGREGATION_FACTORY("count")) over w,
MULTI_AGGREGATE_BY(AsTuple(subkey,value), AGGREGATION_FACTORY("max")) over w
from Input window w as (order by key);
