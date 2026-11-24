/* custom error: No such column: a */
PRAGMA YqlSelect = 'force';

$a = 'a';

SELECT
    x.$a AS member
FROM
    AS_TABLE([<|x: <|a: 1|>|>])
;
