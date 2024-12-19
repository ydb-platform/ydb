/* postgres can not */
/* syntax version 1 */
$makeOpt = Python::makeOpt(
    Callable<(String, Bool) -> String?>,
    @@
def makeOpt(arg, flag):
    if flag:
        return arg
    else:
        return None
@@
);

SELECT
    *
FROM
    plato.Input
WHERE
    key >= $makeOpt('030', TRUE) AND key <= '100'
;

SELECT
    *
FROM
    plato.Input
WHERE
    key >= $makeOpt('030', FALSE) AND key <= '100'
;
