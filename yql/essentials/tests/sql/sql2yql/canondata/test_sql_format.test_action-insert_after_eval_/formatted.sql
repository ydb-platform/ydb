/* syntax version 1 */
/* postgres can not */
/* multirun can not */
USE plato;

$s1 = (
    SELECT
        count(*)
    FROM Output
);

$s2 = (
    SELECT
        max(key)
    FROM Output
);

INSERT INTO Output WITH truncate
SELECT
    EvaluateExpr($s1) AS a,
    EvaluateExpr($s2) AS b;
