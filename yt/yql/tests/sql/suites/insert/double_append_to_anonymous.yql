/* postgres can not */
use plato;

INSERT INTO @tmp
SELECT 1 as id, "qwer" as val;
COMMIT;

INSERT INTO @tmp
SELECT 2 as id, "asdf" as val;

COMMIT;

SELECT *
FROM @tmp;