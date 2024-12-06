USE plato;
PRAGMA yt.MaxInputTables = "2";

SELECT
    key AS key,
    "" AS subkey,
    "value:" || value AS value
FROM
    concat(Input, Input, Input, Input)
WHERE
    key == "07" || "5"
;
