/* syntax version 1 */
select
    DateTime::ToSeconds(`date`) as date_to_seconds,
    DateTime::ToSeconds(`interval`) as interval_to_seconds
from (
    select
        cast(fdate as Date) as `date`,
        cast(finterval as Interval) as `interval`
    from Input
);
