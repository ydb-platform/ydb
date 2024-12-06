/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO Output WITH truncate (
    a,
    b
)
VALUES
    ('00', '10'),
    ('11', '20'),
    ('21', '30'),
    ('31', '40'),
    ('41', '50');

COMMIT;

INSERT INTO Output
SELECT
    *
FROM
    Output
ORDER BY
    a
;

COMMIT;

SELECT
    *
FROM
    Output
WHERE
    a > '11'
;
