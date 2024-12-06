/* syntax version 1 */
/* postgres can not */
USE plato;

$c = "cccc";

INSERT INTO @$c
SELECT
    1 AS x
;

COMMIT;

SELECT
    *
FROM
    @$c
;
