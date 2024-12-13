/* syntax version 1 */
/* postgres can not */
/* custom error: Table  "Output" is used before commit */
USE plato;

INSERT INTO Output
SELECT
    'key' AS field
UNION ALL
SELECT
    'subkey' AS field
;

COMMIT;

$whitelist =
    SELECT
        aggregate_list(field)
    FROM
        Output
;

SELECT
    ForceSpreadMembers([('key', key)], Unwrap($whitelist))
FROM
    Input
;
