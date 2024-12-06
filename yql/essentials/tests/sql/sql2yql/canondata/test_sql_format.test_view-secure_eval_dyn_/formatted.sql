/* custom error:FuncCode should have constant function name in views*/
PRAGMA yt.ViewIsolation = 'true';
USE plato;

SELECT
    k,
    s,
    v
FROM
    Input VIEW secure_eval_dynamic
;
