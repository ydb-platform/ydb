/* postgres can not */
/* hybridfile can not YQL-17743 */
USE plato;

PRAGMA OrderedColumns;
PRAGMA yt.UseNativeYtTypes;

insert into @tmp
select key, AsList(subkey), value
from Input
where key > '000'
order by value desc;
