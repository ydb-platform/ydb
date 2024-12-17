$foo = YQL::@@(lambda '(item) (Concat (String '@@@@foo@@@@@@@@
@@@@) item))@@;

SELECT
    $foo('bar')
;
