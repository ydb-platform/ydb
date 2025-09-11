/* syntax version 1 */
/* postgres can not */

PRAGMA yt.JoinMergeTablesLimit="10";
PRAGMA yt.JoinAllowColumnRenames="true";
PRAGMA yt.JoinMergeUnsortedFactor="5.0";

USE plato;

$t = [<|"x":"bbb", "y":1, "z":Null|>, <|"x":"bbb", "y":2, "z":Null|>];

SELECT * FROM Input1 AS a LEFT JOIN AS_TABLE($t) AS b ON a.k1 = b.x ORDER BY k1, y;

