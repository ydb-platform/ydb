/* syntax version 1 */
$format = DateTime::Format("%Y%m%d %H%M%S %Z");

select
    $format(`date`),
    $format(`datetime`),
    $format(`timestamp`),
    $format(`tzdate`),
    $format(`tzdatetime`),
    $format(`tztimestamp`)
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
