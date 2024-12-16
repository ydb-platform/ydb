/* syntax version 1 */
pragma UseBlocks;
insert into @t
    select
        Unwrap(cast(fdate as Date)) as `date`,
        Unwrap(cast(fdatetime as Datetime)) as `datetime`,
        Unwrap(cast(ftimestamp as Timestamp)) as `timestamp`,
        Unwrap(cast(ftzdate as TzDate)) as `tzdate`,
        Unwrap(cast(ftzdatetime as TzDatetime)) as `tzdatetime`,
        Unwrap(cast(ftztimestamp as TzTimestamp)) as `tztimestamp`
    from Input;
commit;

select
    DateTime::MakeDate(`date`) as rdate,
    DateTime::MakeDatetime(`datetime`) as rdatetime,
    DateTime::MakeTimestamp(`timestamp`) as rtimestamp,
    DateTime::MakeTzDate(`tzdate`) as rtzdate,
    DateTime::MakeTzDatetime(`tzdatetime`) as rtzdatetime,
    DateTime::MakeTzTimestamp(`tztimestamp`) as rtztimestamp
from @t;