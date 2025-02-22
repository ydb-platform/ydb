/* ignore runonopt plan diff */
PRAGMA OrderedColumns;
PRAGMA yt.UseNativeYtTypes;
USE plato;

INSERT INTO @table1 WITH TRUNCATE (a, c, b) VALUES ('1', '2', '3');
COMMIT;

INSERT INTO Output WITH TRUNCATE 
SELECT x.c AS d, x.b AS b, json('{}') AS a
FROM @table1 AS x
ORDER BY d
