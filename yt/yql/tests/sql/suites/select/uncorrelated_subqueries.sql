/* postgres can not */
use plato;

$_data = (SELECT key as kk, subkey as sk, value as val FROM plato.Input1 WHERE key == '075');
$data_one_key = (SELECT subkey FROM plato.Input1 WHERE key == '075');

SELECT * FROM Input2 WHERE key = $data_one_key;
