use plato;

SELECT
    a.*,
FROM CONCAT(InputOpt1, InputOpt2, InputOpt3, InputOpt4) AS a
LEFT SEMI JOIN Input AS b
ON COALESCE(a.key, a.value) == b.key
    AND COALESCE(a.subkey, '1') == b.subkey
WHERE
    COALESCE(a.subkey, '1') == '1'
;
