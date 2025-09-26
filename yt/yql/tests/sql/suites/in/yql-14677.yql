/* postgres can not */
USE plato;
pragma yt.MapJoinLimit="1m";

$l1 = select key from `Input`;

select * from Input
where true
  and value != ""
  and key in $l1
