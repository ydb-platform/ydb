$foo = ($item) -> {
    RETURN $item + $item;
};

SELECT
    $foo(1)
;
