/* syntax version 1 */
/* postgres can not */
USE plato;

$arg1 = '' || '';

$arg2 = ($_item) -> {
    RETURN TRUE;
};

$arg3 = '' || '';
$arg4 = '' || 'raw';

SELECT
    count(*)
FROM
    FILTER($arg1, $arg2, $arg3, $arg4)
;
