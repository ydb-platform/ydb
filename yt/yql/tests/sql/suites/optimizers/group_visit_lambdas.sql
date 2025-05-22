/* postgres can not */

USE plato;

select
    key,
    subkey,
    value,
    TablePath() as path
from
    range("", "Input1", "Input5")
where key != ""
order by key, subkey, path;