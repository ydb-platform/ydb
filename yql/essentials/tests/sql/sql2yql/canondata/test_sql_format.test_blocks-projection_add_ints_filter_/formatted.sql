SELECT
    key,
    subkey + 1
FROM
    as_table([<|key: 1, subkey: 2|>])
WHERE
    key == 1
;
