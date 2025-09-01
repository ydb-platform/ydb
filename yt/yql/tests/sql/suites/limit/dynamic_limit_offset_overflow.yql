-- YQL-19579
-- Check that offset + limit don't overflow max uin64
use plato;

$limit = -1;
$offset = 2;
$limit = if($limit >= 0, cast($limit as uint64));
$offset = if($offset >= 0, cast($offset as uint64));

$i = select distinct key from Input;

select * from $i order by key
limit $limit offset $offset;

