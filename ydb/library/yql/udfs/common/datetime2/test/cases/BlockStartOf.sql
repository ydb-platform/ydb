/* syntax version 1 */
pragma UseBlocks;
insert into @t
    select
        cast(ftztimestamp as TzTimestamp) as `tztimestamp`,
    from Input;

commit;

select
    DateTime::StartOfYear(`tztimestamp`),

    DateTime::StartOfQuarter(`tztimestamp`),

    DateTime::StartOfMonth(`tztimestamp`),

    DateTime::StartOfWeek(`tztimestamp`),

    DateTime::StartOfDay(`tztimestamp`),

    DateTime::StartOf(`tztimestamp`, Interval("PT13H")),

    DateTime::StartOf(`tztimestamp`, Interval("PT4H")),
    DateTime::StartOf(`tztimestamp`, Interval("PT15M")),
    DateTime::StartOf(`tztimestamp`, Interval("PT20S")),
    DateTime::StartOf(`tztimestamp`, Interval("PT7S")),
    DateTime::TimeOfDay(`tztimestamp`),

    DateTime::EndOfMonth(`tztimestamp`),
from @t;
