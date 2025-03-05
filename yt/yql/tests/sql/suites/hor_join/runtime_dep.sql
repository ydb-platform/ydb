/* postgres can not */
use plato;

INSERT INTO @ttt WITH TRUNCATE
SELECT CAST(key AS int) as key, subkey, value FROM Input;

COMMIT;

SELECT * FROM (
    SELECT key, '' as value FROM @ttt
    UNION ALL
    SELECT 0 as key, value from @ttt
) AS x
ORDER BY key, value
;
