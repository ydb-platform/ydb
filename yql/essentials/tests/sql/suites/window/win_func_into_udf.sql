/* postgres can not */
SELECT
   key, Math::Sqrt(CAST(row_number() over w as double)) as sq
FROM plato.Input
WINDOW w AS (partition by key order by subkey)
ORDER BY key, sq;
