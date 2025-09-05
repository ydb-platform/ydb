/* syntax version 1 */
PRAGMA DisableSimpleColumns;
/* postgres can not */
use plato;

$data_dict = (select
    mod,
    YQL::ToIndexDict(ListSort(aggregate_list(key))) as dk,
    ListSort(aggregate_list(subkey)) as ls,
    ListSort(aggregate_list(value)) as lv
from Input
group by cast(key as uint32) % 10 as mod);

--INSERT INTO Output
SELECT
--DISTINCT
    dk[2],
    key,
    value
FROM Input JOIN $data_dict AS d on cast(Input.key as uint32) / 100 == d.mod
order by key, value
;
