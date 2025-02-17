/* syntax version 1 */
/* postgres can not */
DO BEGIN
    SELECT
        1
    ;
END DO;

EVALUATE IF TRUE DO BEGIN
    SELECT
        1
    ;
END DO ELSE DO BEGIN
    SELECT
        2
    ;
END DO;

EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
    SELECT
        $i
    ;
END DO;
