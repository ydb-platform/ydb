SELECT key, MIN(value) FROM Input GROUP BY key ORDER BY key;

SELECT * FROM Input;
SELECT CAST(key AS Uint32) + 1 AS intkey, subkey FROM Input;
SELECT value, key FROM Input WHERE CAST(key AS Uint32) < 100;
