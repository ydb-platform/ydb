/* syntax version 1 */
/* multirun can not */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";
pragma yt.NativeYtTypeCompatibility="complex";

insert into Output
select t.*, CAST(TableRecordIndex() as UInt64) as record_idx
from Input as t
order by record_idx;
