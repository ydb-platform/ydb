/* postgres can not */
/* syntax version 1 */
$l = ($x) -> (ListMap($x, Yson::Serialize));
$d = ($x) -> (ToDict(ListMap(DictItems($x), ($i) -> (($i.0, Yson::Serialize($i.1))))));

SELECT
    $l($l(Yson::ConvertToList(Yson('[1;2;3]'))))
;

SELECT
    ListSort(DictItems($d($d(Yson::ConvertToDict(Yson('{a=1;b=2}'))))), ($x) -> ($x.0))
;
