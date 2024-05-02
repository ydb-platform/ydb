/* syntax version 1 */

SELECT ListTop([4, 2, 3, 1], 2);
SELECT ListTop([4, 2, 3, 1], 2, ($x) -> {return -$x; });
SELECT ListTopSort([4, 2, 3, 1], 2);
SELECT ListTopSort([4, 2, 3, 1], 2, ($x) -> {return -$x; });