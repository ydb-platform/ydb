/* syntax version 1 */
select
    DateTime::MakeDate32(DateTime::Split64(dt)) as dt
from (
    select
        cast(dt as Date32) as dt
    from Input
);
