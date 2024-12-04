/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;


$src =
select Date("2022-01-01") as int32, 2 as value
union all
select Date("2021-12-31") as int32, 1 as value;

$with_bytes = select t.*, ToBytes(int32) as date_bytes, ToBytes(value) as int_bytes from $src as t;

select
 int32,
 value,
 FromBytes(date_bytes, TypeOf(int32)) as d,
 FromBytes(int_bytes, int32) as i
from $with_bytes
order by int32;
