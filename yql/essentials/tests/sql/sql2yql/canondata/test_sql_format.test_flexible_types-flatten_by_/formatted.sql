/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

SELECT
    *
FROM (
    SELECT
        [1, 1, 1] AS text
)
    FLATTEN LIST BY (
        text
    )
;
