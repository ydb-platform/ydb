/* syntax version 1 */
/* multirun can not */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";
pragma yt.NativeYtTypeCompatibility="date";

insert into Output
select key || "a" as key, subkey
from Input
order by key;
