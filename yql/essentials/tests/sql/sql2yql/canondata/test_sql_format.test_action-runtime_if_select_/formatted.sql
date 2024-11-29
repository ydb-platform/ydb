/* syntax version 1 */
/* postgres can not */
USE plato;

INSERT INTO @foo
SELECT
    count(*) AS count
FROM Input;
COMMIT;

$n = (
    SELECT
        count
    FROM @foo
);
$predicate = $n > 1;

IF $predicate
    DO BEGIN
        SELECT
            1;
    END DO;

IF NOT $predicate
    DO BEGIN
        SELECT
            2;
    END DO;

IF $predicate
    DO BEGIN
        SELECT
            3;
    END DO
ELSE
    DO BEGIN
        SELECT
            4;
    END DO;

IF NOT $predicate
    DO BEGIN
        SELECT
            5;
    END DO
ELSE
    DO BEGIN
        SELECT
            6;
    END DO;
