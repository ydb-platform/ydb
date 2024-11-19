/* postgres can not */
/* syntax version 1 */
$l = ($x)->( ListMap($x, Yson::Serialize) );
$d = ($x)->( ToDict(ListMap(DictItems($x),($i)->(($i.0,Yson::Serialize($i.1))))));

select $l($l(Yson::ConvertToList(Yson("[1;2;3]"))));
select ListSort(DictItems($d($d(Yson::ConvertToDict(Yson("{a=1;b=2}"))))), ($x)->($x.0));
