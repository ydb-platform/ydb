/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

$t = SELECT * FROM AS_TABLE([<|key: 1, value: 2|>,<|key: 1, value: 2|>]);

$p =
SELECT
    key,some(value) as a
FROM $t
GROUP BY
    key
    with combine
    ;

$p = PROCESS $p;
--select FormatType(TypeOf($p));
select * from $p;

$p = SELECT
    key,some(a) as a
FROM $p
GROUP BY
    key
    with combinestate
    ;

$p = PROCESS $p;
--select FormatType(TypeOf($p));
select * from $p;

$p = SELECT
    key,some(a) as a
FROM $p
GROUP BY
    key
    with mergestate
    ;

$p = PROCESS $p;
--select FormatType(TypeOf($p));
select * from $p;


$p1 = SELECT
    key,some(a) as a
FROM $p
GROUP BY
    key
    with mergefinalize
    ;

$p1 = PROCESS $p1;
--select FormatType(TypeOf($p1));
select * from $p1;

$p2 = SELECT
    key,some(a) as a
FROM (select key, just(a) as a from $p)
GROUP BY
    key
    with mergemanyfinalize
    ;

$p2 = PROCESS $p2;
--select FormatType(TypeOf($p2));
select * from $p2;

$p3 =
SELECT
    key,some(value) as a
FROM $t
GROUP BY
    key
    with finalize
    ;

$p3 = PROCESS $p3;
--select FormatType(TypeOf($p));
select * from $p3;
