/* postgres can not */
use plato;

$avg = (SELECT AVG(Length(key)) FROM Input);

SELECT key FROM Input LIMIT Cast($avg as Uint64) ?? 0;
