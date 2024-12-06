USE plato;

$lst =
    PROCESS Input;
$dict = ToDict(ListMap($lst, ($x) -> (($x.key, $x.subkey))));

SELECT
    DictLength($dict)
;
