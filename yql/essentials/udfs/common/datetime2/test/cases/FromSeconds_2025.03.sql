/* syntax version 1 */
select
    DateTime::IntervalFromSeconds(`seconds`) as interval_from_seconds
from (
    select
        cast(fseconds as Int64) as `seconds`
    from Input
);
