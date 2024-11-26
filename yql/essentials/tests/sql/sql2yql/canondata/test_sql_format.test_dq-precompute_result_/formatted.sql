USE plato;

$l =
    SELECT
        min(CAST(key AS uint64)) AS key
    FROM Input;
$l = EvaluateExpr($l);

SELECT
    *
FROM Input
LIMIT $l ?? 0;
