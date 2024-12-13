/* syntax version 1 */
SELECT
    key AS key,
    subkey AS subkey,
    Input.`dict`['a'] AS a,
    Input.`dict`['b'] AS b,
    Input.`dict`['c'] AS c,
    Input.`dict`['d'] AS d
FROM
    plato.Input
;
