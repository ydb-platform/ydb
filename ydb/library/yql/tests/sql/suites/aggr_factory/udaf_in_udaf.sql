/* syntax version 1 */

$merge_dicts = ($dict1, $dict2) -> { return SetUnion($dict1, $dict2, ($_key, $a, $b) -> { RETURN Coalesce($a, 0) + Coalesce($b, 0) }) };

$create_single_item_dict = ($item, $_parent) -> { return AsDict(AsTuple($item, 1)) };
$count_values = AGGREGATION_FACTORY(
    "UDAF",
    $create_single_item_dict,
    ($dict, $item, $parent) -> { return $merge_dicts($create_single_item_dict($item, $parent), $dict) },
    $merge_dicts
);

$create_dict_from_list = ($list, $_parent) -> { return ListAggregate($list, $count_values) };
$add_list_to_dict = ($dict, $list, $parent) -> { return $merge_dicts($create_dict_from_list($list, $parent), $dict) };
$count_list_values = AGGREGATION_FACTORY(
    "UDAF",
    $create_dict_from_list,
    $add_list_to_dict,
    $merge_dicts
);

$test_data = AsList(AsList(1,2),AsList(3,2),AsList(3,3),AsList(1,3),AsList(3,1),AsList(2,2));
SELECT
    ListSort(DictItems(ListAggregate(AsList(1,2,3,2,3,3), $count_values))) AS count_values,
    ListSort(DictItems(ListAggregate($test_data, $count_list_values))) AS count_list_values,
