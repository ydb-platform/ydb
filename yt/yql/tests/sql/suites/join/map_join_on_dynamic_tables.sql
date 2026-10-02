USE plato;

PRAGMA yt.MapJoinLimit='1M';
PRAGMA yt.ConvertDynamicTablesToStatic='join';

SELECT a.*,
FROM Input1 as a
LEFT JOIN Input2 as b
ON a.key == b.subkey;
