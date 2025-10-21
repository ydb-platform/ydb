/* postgres can not */
/* syntax version 1 */
$s = <| a:1, b:2, c:3, d:4 |>;
$list = ["a", "b"];
select * from (select CombineMembers(
    ChooseMembers($s, $list), 
    <| remain : RemoveMembers($s, $list) |>
    )) flatten columns;
    