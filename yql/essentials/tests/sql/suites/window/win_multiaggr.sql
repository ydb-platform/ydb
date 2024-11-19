/* postgres can not */
use plato;

select MULTI_AGGREGATE_BY(AsStruct(subkey as a,value as b), AGGREGATION_FACTORY("count")) over w,
MULTI_AGGREGATE_BY(AsStruct(subkey as a,value as b), AGGREGATION_FACTORY("max")) over w
from Input window w as (order by key);
