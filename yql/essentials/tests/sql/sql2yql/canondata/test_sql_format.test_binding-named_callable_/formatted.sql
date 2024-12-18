/* syntax version 1 */
/* postgres can not */
$foo = ($item) -> {
    RETURN $item + $item;
};

SELECT
    $foo(1)
;
