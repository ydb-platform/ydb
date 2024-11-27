USE plato;

$f = ($x, $optArg?)->{
    return Ensure($x, $optArg is null or len($optArg)>0);
};

PROCESS Input0 USING $f(TableRow());

PROCESS Input0 USING $f(TableRow(),'foo');

PROCESS Input0 USING $f(TableRows());

PROCESS Input0 USING $f(TableRows(),'foo');

