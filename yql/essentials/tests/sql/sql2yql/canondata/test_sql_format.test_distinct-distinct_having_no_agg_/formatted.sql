/* syntax version 1 */
PRAGMA warning("disable", "4526");
USE plato;

SELECT DISTINCT
    key
FROM Input2
HAVING key != '0';
