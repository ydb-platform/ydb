/* syntax version 1 */
$format = DateTime::Format("%% year %Y monthFullName %B monthShortName %b month %m day %d hours %H minutes %M seconds %S tz %z tzname %Z text");

select
    $format(DateTime::Split(cast(ftztimestamp as TzTimestamp)))
from Input
