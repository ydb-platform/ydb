/* postgres can not */
select
    key, subkey,
    nanvl(correlation(cast(key as double), cast(subkey as double)) over w, NULL) as corr,
    nanvl(covariance(cast(key as double), cast(subkey as double)) over w, -9.9) as covar,
    hll(value, 18) over w as hll    
from plato.Input
window w as (order by subkey);
