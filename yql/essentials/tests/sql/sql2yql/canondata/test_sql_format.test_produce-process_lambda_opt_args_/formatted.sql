USE plato;
$f = ($x, $optArg?) -> {
    RETURN Ensure($x, $optArg IS NULL OR len($optArg) > 0);
};

PROCESS Input0
USING $f(TableRow());

PROCESS Input0
USING $f(TableRow(), 'foo');

PROCESS Input0
USING $f(TableRows());

PROCESS Input0
USING $f(TableRows(), 'foo');
