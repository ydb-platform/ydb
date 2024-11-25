/* custom error:FuncCode should have constant function name in views*/
pragma yt.ViewIsolation = 'true';
USE plato;
SELECT k, s, v FROM Input VIEW secure_eval_dynamic;
