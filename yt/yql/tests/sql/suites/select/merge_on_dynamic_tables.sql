USE plato;

PRAGMA yt.ConvertDynamicTablesToStatic='all';

SELECT value + 1,
FROM Input;
