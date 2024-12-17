/* syntax version 1 */
$parse1 = DateTime::Parse("%Y %m %d %H%M%S %Z text");
$parse2 = DateTime::Parse("%%text%% %m/%d/%Y %H:%M:%S");
$parse3 = DateTime::Parse("%B/%d/%Y");
$parse4 = DateTime::Parse("%b/%d/%Y");


$format1 = DateTime::Format("%Y-%m-%dT%H:%M:%S,%Z");
$format2 = DateTime::Format("%Y%m%d %H%M%S %z");
$format3 = DateTime::Format("%Y%m%d");

select
    $format1($parse1(fdatetime1)),
    $format2($parse1(fdatetime1)),
    $format1($parse2(fdatetime2)),
    $format2($parse2(fdatetime2)),

    $format3($parse3(fdatetime3)),
    $format3($parse4(fdatetime4)),
    
    $format1(DateTime::ParseRfc822(frfc822)),
    $format1(DateTime::ParseIso8601(fiso8601)),
    $format1(DateTime::ParseHttp(fhttp)),
    $format1(DateTime::ParseX509(fx509))
from Input
