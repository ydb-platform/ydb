/* syntax version 1 */

$list = [4, 2, 3, 1];

SELECT ListSort(ListTop($list, 2));
SELECT ListSort(ListTop($list, 2, ($x) -> {return -$x; }));

SELECT ListTopSort($list, 2);
SELECT ListTopSort($list, 2, ($x) -> {return -$x; });
SELECT ListTopDesc($list, 2);
SELECT ListTopSortDesc($list, 2);

SELECT ListTopSort(NULL, 1);
SELECT ListTopSort([], 0);
SELECT ListTopSortDesc(Cast($list AS Optional<List<Uint64>>), 2);
