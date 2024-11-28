/* syntax version 1 */
USE plato;
$foo = [<|"x": 1|>];
$bar = [<|"x": 1, "y": NULL|>];

INSERT INTO Output
SELECT
    *
FROM AS_TABLE($foo)
    AS a
LEFT JOIN AS_TABLE($bar)
    AS b
USING (x);
