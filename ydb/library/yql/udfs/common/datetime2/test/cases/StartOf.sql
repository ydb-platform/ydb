/* syntax version 1 */
$format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");

select
    $format(DateTime::StartOfYear(`tztimestamp`)),
    $format(DateTime::StartOfQuarter(`tztimestamp`)),
    $format(DateTime::StartOfMonth(`tztimestamp`)),
    $format(DateTime::StartOfWeek(`tztimestamp`)),
    $format(DateTime::StartOfDay(`tztimestamp`)),
    $format(DateTime::StartOf(`tztimestamp`, Interval("PT13H"))),
    $format(DateTime::StartOf(`tztimestamp`, Interval("PT4H"))),
    $format(DateTime::StartOf(`tztimestamp`, Interval("PT15M"))),
    $format(DateTime::StartOf(`tztimestamp`, Interval("PT20S"))),
    $format(DateTime::StartOf(`tztimestamp`, Interval("PT7S"))),
    DateTime::TimeOfDay(`tztimestamp`),
    $format(DateTime::EndOfMonth(`tztimestamp`)),
from (
    select
        cast(ftztimestamp as TzTimestamp) as `tztimestamp`
    from Input
);
