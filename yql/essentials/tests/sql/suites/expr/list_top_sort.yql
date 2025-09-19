/* syntax version 1 */

$list = [45, 20, 34, 16];
$empty = [];

$digit = ($x) -> { return $x % 10; };

SELECT 
    ListTopSort($list, 2), ListTopSort($list, 4), ListTopSort($list, 5), 
    ListTopSort($list, 2, $digit), 
    ListTopSort(NULL, 2), ListTopSort(Just($list), 2), ListTopSort($empty, 0);
SELECT
    ListTopSortAsc($list, 2), ListTopSortAsc($list, 4), ListTopSortAsc($list, 5), 
    ListTopSortAsc($list, 2, $digit), 
    ListTopSortAsc(NULL, 2), ListTopSortAsc(Just($list), 2), ListTopSortAsc($empty, 0);
SELECT 
    ListTopSortDesc($list, 2), ListTopSortDesc($list, 4), ListTopSortDesc($list, 5),
    ListTopSortDesc($list, 2, $digit),
    ListTopSortDesc(NULL, 2), ListTopSortDesc(Just($list), 2), ListTopSortDesc($empty, 0);

SELECT 
    ListSort(ListTop($list, 2)), ListSort(ListTop($list, 4)), ListSort(ListTop($list, 5)), 
    ListSort(ListTop($list, 2, $digit)), 
    ListSort(ListTop(NULL, 2)), ListSort(ListTop(Just($list), 2)), ListSort(ListTop($empty, 0));
SELECT
    ListSort(ListTopAsc($list, 2)), ListSort(ListTopAsc($list, 4)), ListSort(ListTopAsc($list, 5)), 
    ListSort(ListTopAsc($list, 2, $digit)), 
    ListSort(ListTopAsc(NULL, 2)), ListSort(ListTopAsc(Just($list), 2)), ListSort(ListTopAsc($empty, 0));
SELECT 
    ListSort(ListTopDesc($list, 2)), ListSort(ListTopDesc($list, 4)), ListSort(ListTopDesc($list, 5)),
    ListSort(ListTopDesc($list, 2, $digit)),
    ListSort(ListTopDesc(NULL, 2)), ListSort(ListTopDesc(Just($list), 2)), ListSort(ListTopDesc($empty, 0));