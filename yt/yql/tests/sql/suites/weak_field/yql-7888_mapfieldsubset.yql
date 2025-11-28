/* postgres can not */
/* syntax version 1 */
use plato;

define subquery $input() as
    select
            value,
            WeakField(strongest_id, "String") as strongest_id,
            WeakField(video_position_sec, "String") as video_position_sec,
            key,
            subkey
        from concat(Input, Input)
        where key in ("heartbeat", "show", "click")
            and subkey in ("native", "gif");
end define;

-- Native:
define subquery $native_show_and_clicks($input) as
    select 
        value, strongest_id, key
        from $input()
        where subkey == "native"
            and key in ("click", "show");
end define;

select count(distinct strongest_id) as native_users from $native_show_and_clicks($input);
select count(distinct strongest_id) as native_users_with_click from $native_show_and_clicks($input) where key == "click";