/* custom error:Duplicated member: kk*/
PRAGMA SimpleColumns;

USE plato;

$data = (
    SELECT
        key AS kk,
        subkey AS sk,
        value AS val
    FROM
        Input
    WHERE
        CAST(key AS uint32) / 100 < 5
);

--INSERT INTO Output
SELECT
    d.*,
    Input.key AS kk -- 'kk' is exist from d.kk
FROM
    Input
JOIN
    $data AS d
ON
    Input.subkey == CAST(CAST(d.kk AS uint32) / 100 AS string)
ORDER BY
    key,
    val
;
