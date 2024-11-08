/* syntax version 1 */

pragma UseBlocks;
insert into @t
    select
        cast(finterval1 as Interval) as `interval1`,
        cast(finterval2 as Interval) as `interval2`,
        cast(finterval3 as Interval) as `interval3`,
        cast(finterval4 as Interval) as `interval4`
from Input;

commit;
        
select
    DateTime::ToDays(`interval1`) as `interval1`,
    DateTime::ToDays(`interval2`) as `interval2`,
    DateTime::ToDays(`interval3`) as `interval3`,
    DateTime::ToDays(`interval4`) as `interval4`
from @t;

