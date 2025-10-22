/* syntax version 1 */
/* postgres can not */
use plato;

$data = (SELECT key, Just(aggregate_list(cast(subkey as uint32))) as lsk FROM plato.Input GROUP BY cast(key as uint32) as key);

SELECT key, lsk[0] FROM $data WHERE lsk IS NOT NULL ORDER BY key;
