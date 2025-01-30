/* postgres can not */
use plato;

$_data = (SELECT key as kk, subkey as sk, value as val FROM plato.Input WHERE key == '075');
$data_one_key = (SELECT subkey as sk FROM plato.Input WHERE key == '075');

SELECT * FROM Input WHERE key = $data_one_key;
