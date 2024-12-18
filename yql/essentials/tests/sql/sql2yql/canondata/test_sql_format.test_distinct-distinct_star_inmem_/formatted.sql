/* syntax version 1 */
/* postgres can not */
$src = (
    SELECT
        1,
        2,
        3
    UNION ALL
    SELECT
        1,
        2,
        3
);

SELECT DISTINCT
    *
FROM
    $src
;
