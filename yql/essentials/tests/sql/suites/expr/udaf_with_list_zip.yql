/* syntax version 1 */
/* postgres can not */
$lists_2sum = ($l1, $l2) -> (
    ListMap(ListZip($l1, $l2), ($x) -> ($x.0 + $x.1))
);

$perelement_sum = AGGREGATION_FACTORY(
    "UDAF",
    ($item, $_parent) -> ( $item ),
    ($state, $item, $_parent) -> ( $lists_2sum($state, $item) ),
    ($state1, $state2) -> ( $lists_2sum($state1, $state2) )
);

SELECT
    AGGREGATE_BY(list_col, $perelement_sum) AS cnt1,
FROM AS_TABLE([
<|"list_col" : [4, 5, 6]|>,
<|"list_col" : [4, 5, 6]|>,
<|"list_col" : [4, 5, 6]|>,
<|"list_col" : [4, 5, 6]|>,
<|"list_col" : [4, 5, 6]|>,
<|"list_col" : [4, 5, 6]|>
]);
