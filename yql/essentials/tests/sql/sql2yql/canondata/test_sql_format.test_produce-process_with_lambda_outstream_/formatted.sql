/* syntax version 1 */
/* postgres can not */
USE plato;
$f1 = ($r) -> {
    RETURN $r;
};

PROCESS Input0
USING $f1(TableRow());
$f2 = ($r) -> {
    RETURN Just($r);
};

PROCESS Input0
USING $f2(TableRow());
$f3 = ($r) -> {
    RETURN AsList($r, $r);
};

PROCESS Input0
USING $f3(TableRow());
$f4 = ($r) -> {
    RETURN Yql::Iterator(AsList($r, $r));
};

PROCESS Input0
USING $f4(TableRow());
