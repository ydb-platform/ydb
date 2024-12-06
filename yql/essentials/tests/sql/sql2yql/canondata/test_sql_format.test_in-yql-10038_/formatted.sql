/* postgres can not */
USE plato;

INSERT INTO @input
SELECT
    "foo" AS reqid,
    "touch" AS ui,
    AsList(1, 2, 236273) AS test_ids
;
COMMIT;

$dict = (
    SELECT
        "foo" AS reqid
);

SELECT
    *
FROM
    @input
WHERE
    ui == 'touch'
    AND reqid IN (
        SELECT
            reqid
        FROM
            $dict
    )
    AND 236273 IN test_ids
;
