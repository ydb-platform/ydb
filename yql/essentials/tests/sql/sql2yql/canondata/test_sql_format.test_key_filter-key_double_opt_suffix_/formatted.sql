USE plato;

INSERT INTO Output
SELECT
    1 AS key,
    Just(Just("x")) AS subkey,
UNION ALL
SELECT
    2 AS key,
    NULL AS subkey,
UNION ALL
SELECT
    2 AS key,
    Just(Nothing(String?)) AS subkey,
ORDER BY
    key,
    subkey
;

COMMIT;

SELECT
    key,
    subkey
FROM
    Output
WHERE
    key == 2
;
