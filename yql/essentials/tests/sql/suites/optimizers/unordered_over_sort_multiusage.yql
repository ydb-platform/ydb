pragma warning("disable", "4510");
pragma config.flags('OptimizerFlags', 'UnorderedOverSortImproved');

$l = ListSort(ListFromRange(1, 5), ($x)->(-$x));
select ListSum(YQL::Unordered($l));
select ListTake($l, 3);
