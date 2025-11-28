/* syntax version 1 */
/* multirun can not */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";
pragma yt.NativeYtTypeCompatibility="complex";

pragma yt.MaxInputTables="2";

insert into Output with truncate
select
    key
from range("")
where key > "000";
