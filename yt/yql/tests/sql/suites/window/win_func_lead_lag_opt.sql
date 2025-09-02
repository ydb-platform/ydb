/* postgres can not */
use plato;

select
    subkey,
    lag(Just(subkey)) over w as opt_lag,
    lead(Just(subkey)) over w as opt_lead,
    lag(subkey, 0) over w as lag0,
    lead(subkey, 0) over w as lead0
from Input window w as ()
order by subkey;


select
    key,
    lag(optkey) over w as opt_lag,
    lead(Just(optkey)) over w as opt_lead,
    lag(Just(optkey), 0) over w as lag0,
    lead(optkey, 0) over w as lead0
from InputOpt window w as ()
order by key;

select lead(null) over w from (select 1 as key) window w as ();
