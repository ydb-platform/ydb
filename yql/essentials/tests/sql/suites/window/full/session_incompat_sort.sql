/* syntax version 1 */
/* postgres can not */
USE plato;

-- add non-optional partition key
$src = SELECT t.*, user ?? "u0" as user_nonopt FROM Input as t;

SELECT
    user,
    user_nonopt,
    ts,
    payload,
    AGGREGATE_LIST(TableRow()) over w as full_session,
    COUNT(1) over w as session_len,
FROM $src
WINDOW w AS (
    PARTITION BY user, user_nonopt, SessionWindow(ts, 10)
    ORDER BY ts DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY user, payload;
