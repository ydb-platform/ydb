USE plato;

pragma yt.MaxInputTables="2";
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM concat(Input, Input, Input, Input)
WHERE key = "07" || "5"
;