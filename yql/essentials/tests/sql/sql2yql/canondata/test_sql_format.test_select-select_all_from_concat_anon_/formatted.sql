/* postgres can not */
USE plato;

INSERT INTO @foo
SELECT
    1
;
COMMIT;
$name = "fo" || "o";

SELECT
    *
FROM
    concat(@foo, @$name)
;
