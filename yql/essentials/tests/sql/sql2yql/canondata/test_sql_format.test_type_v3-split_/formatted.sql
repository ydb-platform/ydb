USE plato;
PRAGMA yt.MaxInputTables = "2";

INSERT INTO Output
SELECT
    *
FROM
    concat(Input1, Input2, Input3)
;
