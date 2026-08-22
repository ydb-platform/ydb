$parseNginx = DateTime::Parse("%d/%b/%Y:%H:%M:%S %z");
$parseTzOffset = DateTime::Parse("%Y-%m-%dT%H:%M:%S with %z offset");

$format = DateTime::Format("%Y-%m-%dT%H:%M:%S%z(%Z)");

select
    $format($parseNginx(fnginx)) as onginx,
    $format($parseTzOffset(fdatetime)) as odatetime,
from Input;
