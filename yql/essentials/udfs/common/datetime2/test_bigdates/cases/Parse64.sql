/* syntax version 1 */
$parse1 = DateTime::Parse64("%Y %m %d %H%M%S %Z text");
$parse2 = DateTime::Parse64("%%text%% %m/%d/%Y %H:%M:%S");
$parse3 = DateTime::Parse64("%B/%d/%Y");
$parse4 = DateTime::Parse64("%b/%d/%Y");

select
    CAST(DateTime::MakeTimestamp64($parse1(fdatetime1)) AS String),
    CAST(DateTime::MakeTimestamp64($parse2(fdatetime2)) AS String),
    CAST(DateTime::MakeTimestamp64($parse3(fdatetime3)) AS String),
    CAST(DateTime::MakeTimestamp64($parse4(fdatetime4)) AS String),
from Input
