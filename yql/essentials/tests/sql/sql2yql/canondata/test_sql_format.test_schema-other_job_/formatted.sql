SELECT
    key,
    _other
FROM
    plato.range(``, Input1, Input2)
WHERE
    key != 'fake'
;
