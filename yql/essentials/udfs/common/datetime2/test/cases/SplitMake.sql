/* syntax version 1 */
select
    DateTime::MakeDate(DateTime::Split(`date`)) as rdate,
    DateTime::MakeDatetime(DateTime::Split(`datetime`)) as rdatetime,
    DateTime::MakeTimestamp(DateTime::Split(`timestamp`)) as rtimestamp,
    DateTime::MakeTzDate(DateTime::Split(`tzdate`)) as rtzdate,
    DateTime::MakeTzDatetime(DateTime::Split(`tzdatetime`)) as rtzdatetime,
    DateTime::MakeTzTimestamp(DateTime::Split(`tztimestamp`)) as rtztimestamp
from (
    select
        cast(fdate as Date) as `date`,
        cast(fdatetime as Datetime) as `datetime`,
        cast(ftimestamp as Timestamp) as `timestamp`,
        cast(ftzdate as TzDate) as `tzdate`,
        cast(ftzdatetime as TzDatetime) as `tzdatetime`,
        cast(ftztimestamp as TzTimestamp) as `tztimestamp`
    from Input
);
