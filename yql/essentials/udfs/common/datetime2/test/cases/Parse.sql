$rows = [
    <|fdatetime1: "2011 03 08 010203 Europe/Moscow text", fdatetime2: "%text% 02/23/2022 12:00:00.", fdatetime3: "march/08/2011", fdatetime4: "mar/08/2011", frfc822: "Fri, 4 Mar 2005 19:34:45 EST", fiso8601: "2009-02-14T02:31:30+0300", fhttp: "Sunday, 06-Nov-94 08:49:37 GMT", fx509: "20091014165533Z"|>,
    <|fdatetime1: "2011 03 08 010203.22 Europe/Moscow text", fdatetime2: "%text% 02/23/2022 12:00:00.666666", fdatetime3: "November/08/2011", fdatetime4: "Nov/08/2011", frfc822: "4 Mar 05 19:34 -0330", fiso8601: "2009-09-19 03:37:03.012331+04:00", fhttp: "Sun Nov  6 08:49:37 1994", fx509: "990104074212Z"|>,
    <|fdatetime1: "2011 03 08 010203 Europe/Moscow bar", fdatetime2: "%text% 02/23/2022 12:00:00.999999999", fdatetime3: "JanUAry/08/2011", fdatetime4: "jAN/08/2011", frfc822: "17 Nov 2008 19:34:45", fiso8601: "1990-03-151Y15:16:17.18", fhttp: "1990-03-151Y15:16:17.18", fx509: "500101000000Z"|>,
    <|fdatetime1: "2011 03 08 010203 Europe/Moscow bar", fdatetime2: "%text% 02/23/2022 12:00:00.42", fdatetime3: "JanUArY/08/2011", fdatetime4: "JAN/08/2011", frfc822: "17 Nov 2008 19:34:45", fiso8601: "1990-03-151Y15:16:17.182", fhttp: "1990-03-151Y15:16:17.182", fx509: "500101000000Z"|>,
    <|fdatetime1: "2011 03 08 010203 Europe/Moscow bar", fdatetime2: "%text% 02/23/2022 12:00:00.82387468293473839939483932923", fdatetime3: "JanUArY/08/2011", fdatetime4: "feb/08/2011", frfc822: "17 Nov 2008 19:34:45", fiso8601: "1990-03-151Y15:16:17.182", fhttp: "1990-03-151Y15:16:17.182", fx509: "500101000000Z"|>,
];

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
FROM AS_TABLE($rows)
