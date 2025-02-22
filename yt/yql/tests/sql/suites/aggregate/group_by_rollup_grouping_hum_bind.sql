/* syntax version 1 */
/* postgres can not */
$hum_gr_kv = ($grouping) -> {
  return case $grouping
    when 1 then 'Total By First digit key'
    when 2 then 'Total By First char value'
    when 3 then 'Grand Total'
    else 'Group'
  end; 
};

select count(1) as elements, key_first, val_first, $hum_gr_kv(grouping(key_first, val_first)) as group
from plato.Input group by cube(cast(key as uint32) / 100u as key_first, Substring(value, 1, 1) as val_first)
order by elements, key_first, val_first;
