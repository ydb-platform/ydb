/* syntax version 1 */
/* postgres can not */
FOR $i IN Just(AsList(1, 2, 3)) DO BEGIN
    SELECT
        $i
    ;
END DO ELSE DO BEGIN
    SELECT
        10
    ;
END DO;

FOR $i IN Just(ListCreate(Int32)) DO BEGIN
    SELECT
        $i
    ;
END DO ELSE DO BEGIN
    SELECT
        11
    ;
END DO;

FOR $i IN NULL DO BEGIN
    SELECT
        $i
    ;
END DO ELSE DO BEGIN
    SELECT
        12
    ;
END DO;

FOR $i IN AsList(4) DO BEGIN
    SELECT
        $i
    ;
END DO ELSE DO BEGIN
    SELECT
        13
    ;
END DO;

FOR $i IN ListCreate(String) DO BEGIN
    SELECT
        $i
    ;
END DO ELSE DO BEGIN
    SELECT
        14
    ;
END DO;

FOR $i IN AsList(5) DO BEGIN
    SELECT
        $i
    ;
END DO;

FOR $i IN ListCreate(Bool) DO BEGIN
    SELECT
        $i
    ;
END DO;
