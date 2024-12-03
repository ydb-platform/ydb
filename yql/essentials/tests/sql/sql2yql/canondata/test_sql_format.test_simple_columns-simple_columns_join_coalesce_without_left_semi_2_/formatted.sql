/* syntax version 1 */
USE plato;
PRAGMA SimpleColumns;
PRAGMA CoalesceJoinKeysOnQualifiedAll;

SELECT
    a.*
WITHOUT
    a.key,
    a.value
FROM Input
    AS a
LEFT SEMI JOIN Input
    AS b
USING (key)
ORDER BY
    subkey;

SELECT
    *
WITHOUT
    a.key,
    a.value
FROM Input
    AS a
LEFT SEMI JOIN Input
    AS b
USING (key)
ORDER BY
    subkey;
