pragma warning("disable", "4510");
pragma config.flags('OptimizerFlags', 'UnorderedOverSortImproved');

$l = ListMap(ListFromRange(1, 10), ($x) -> (AsStruct($x + 1 as x, $x as y)));
$top_by_neg_x = ListTake(ListSort($l, ($row)->(-$row.x)), 3);
$y = ListMap($top_by_neg_x, ($row) -> ($row.y));

select ListSum(YQL::Unordered($y));
