use plato;
pragma yt.MapJoinLimit="1M";

$i = select TableRecordIndex() as ind, t.* from Input as t;
$filter = select min(ind) as ind from $i group by subkey;

select
    *
from Input
where TableRecordIndex() in $filter;
