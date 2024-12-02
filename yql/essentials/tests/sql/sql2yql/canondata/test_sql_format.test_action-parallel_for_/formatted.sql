/* yt can not */
USE plato;

EVALUATE PARALLEL FOR $i IN [1, 2, 1, 2, 1]
    DO BEGIN
        INSERT INTO Output
        SELECT
            $i AS a;
    END DO;
COMMIT;

INSERT INTO Output WITH truncate
SELECT
    a
FROM Output
ORDER BY
    a;
