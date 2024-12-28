/* postgres can not */
/* syntax version 1 */
$makeOpt = Python::makeOpt(Callable<(String, Bool)->String?>,
@@
def makeOpt(arg, flag):
    if flag:
        return arg
    else:
        return None
@@
);

select * from plato.Input
where key >= $makeOpt("030", true) and key <= "100"
;

select * from plato.Input
where key >= $makeOpt("030", false) and key <= "100"
;
