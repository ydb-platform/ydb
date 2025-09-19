/* syntax version 1 */
$data = [
  <|id:1, time:1, value:'a'|>,
  <|id:1, time:2, value:null|>,
  <|id:1, time:3, value:null|>,
  <|id:1, time:4, value:'b'|>,
  <|id:1, time:5, value:null|>,
  <|id:2, time:1, value:'c'|>,
  <|id:2, time:2, value:'d'|>,
  <|id:2, time:3, value:null|>,
];

select
  a.*,
  count(value) over w1 as w1,
  max(value) over w2 as w2,
from
  as_table($data) as a
window w1 as (order by time, id),
       w2 as (partition by id)
order by id, time;
