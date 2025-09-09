/* syntax version 1 */
/* postgres can not */
/* custom error: Table "Output" does not exist */
USE plato;

INSERT INTO Output
SELECT
    key || "foo" as key2
FROM Input;

COMMIT;

$input = PROCESS Output;
$c = EvaluateCode(ReprCode(FormatType(TypeOf($input))));
select $c;

INSERT INTO Output WITH TRUNCATE
SELECT
    key || "foo" as key3
FROM Input;

COMMIT;

$input = PROCESS Output;
$c = EvaluateCode(ReprCode(FormatType(TypeOf($input))));
select $c;
