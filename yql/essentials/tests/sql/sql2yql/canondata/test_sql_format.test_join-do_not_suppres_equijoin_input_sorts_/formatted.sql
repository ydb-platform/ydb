USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA DisableSimpleColumns;

SELECT
    *
FROM Input1
    AS t1
CROSS JOIN Input2
    AS t2
WHERE t1.k1 == t2.k1 AND t1.k1 < "zzz";
