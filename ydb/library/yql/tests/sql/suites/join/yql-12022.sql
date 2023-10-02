/* postgres can not */
/* syntax version 1 */
USE plato;

DEFINE SUBQUERY $sub($name) AS
    SELECT * FROM $name
END DEFINE;

SELECT a.key
FROM $sub("Input") AS a
INNER JOIN Input AS b
ON  a.key = b.key
WHERE JoinTableRow().`a.subkey` == "wat"
