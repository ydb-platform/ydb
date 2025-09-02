/* postgres can not */
$modVal = () -> {
    RETURN 2;
};

$filter = ($item) -> {
    RETURN NOT ($item % $modVal() == 0);
};

SELECT
    ListFilter(AsList(1, 2, 3, 4, 5), $filter)
;
