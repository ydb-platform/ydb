/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $bar() AS
    SELECT
        [1, 2] AS ks
    ;
END DEFINE;

SELECT
    key
FROM
    $bar()
    FLATTEN LIST BY ks AS key
ORDER BY
    key
;

SELECT
    key
FROM
    $bar()
    FLATTEN LIST BY (
        ListExtend(ks, [3]) AS key
    )
ORDER BY
    key
;
