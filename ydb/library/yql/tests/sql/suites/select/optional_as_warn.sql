/* syntax version 1 */
/* dq can not */
/* dqfile can not */
/* yt can not */
use plato;

pragma AnsiOptionalAs;

select
  key subkey,
  value v,
from Input order by subkey;


select
  key AS subkey,
  value v,
from Input order by subkey;
