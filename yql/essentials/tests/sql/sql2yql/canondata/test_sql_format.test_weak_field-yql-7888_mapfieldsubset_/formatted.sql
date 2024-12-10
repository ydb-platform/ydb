/* postgres can not */
/* syntax version 1 */
USE plato;

DEFINE SUBQUERY $input() AS
    SELECT
        value,
        WeakField(strongest_id, "String") AS strongest_id,
        WeakField(video_position_sec, "String") AS video_position_sec,
        key,
        subkey
    FROM
        concat(Input, Input)
    WHERE
        key IN ("heartbeat", "show", "click")
        AND subkey IN ("native", "gif")
    ;
END DEFINE;

-- Native:
DEFINE SUBQUERY $native_show_and_clicks($input) AS
    SELECT
        value,
        strongest_id,
        key
    FROM
        $input()
    WHERE
        subkey == "native"
        AND key IN ("click", "show")
    ;
END DEFINE;

SELECT
    count(DISTINCT strongest_id) AS native_users
FROM
    $native_show_and_clicks($input)
;

SELECT
    count(DISTINCT strongest_id) AS native_users_with_click
FROM
    $native_show_and_clicks($input)
WHERE
    key == "click"
;
