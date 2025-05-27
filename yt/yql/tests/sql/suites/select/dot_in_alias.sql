/* postgres can not */
USE plato;
PRAGMA AllowDotInAlias;

--INSERT INTO Output
SELECT key as `.key`, subkey as `sub.key`, value as `value.` FROM Input ORDER BY `.key`, `sub.key`
