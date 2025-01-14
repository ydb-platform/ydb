/* syntax version 1 */
/* postgres can not */
USE plato;

$f1 = ($r)->{
   return $r;
};

PROCESS Input0 USING $f1(TableRow());

$f2 = ($r)->{
   return Just($r);
};

PROCESS Input0 USING $f2(TableRow());

$f3 = ($r)->{
   return AsList($r,$r);
};

PROCESS Input0 USING $f3(TableRow());

$f4 = ($r)->{
   return Yql::Iterator(AsList($r,$r));
};

PROCESS Input0 USING $f4(TableRow());
