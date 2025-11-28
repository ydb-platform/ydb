/* postgres can not */
/* syntax version 1 */
$lst = AsList(AsTuple(13,4),AsTuple(11,2),AsTuple(17,8),AsTuple(5,6));

$p1 = Pickle(ToDict($lst));
$p2 = StablePickle(ToDict($lst));
$p3 = StablePickle(ToSortedDict($lst));
$p4 = Pickle(ToDict(ListReverse($lst)));
$p5 = StablePickle(ToDict(ListReverse($lst)));
select $p1, Ensure($p2, $p1 != $p2), Ensure($p3, $p2 = $p3),
    Ensure($p4, $p4 != $p1), Ensure($p5, $p5 = $p2);
select ListSort(DictItems(Unpickle(TypeOf(ToDict($lst)),$p1))), ListSort(DictItems(Unpickle(TypeOf(ToDict($lst)),$p2)));

