$foo = YQL::@@(lambda '(item) (+ item item))@@;

SELECT
    $foo(1)
;
