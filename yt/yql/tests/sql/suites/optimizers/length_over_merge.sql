/* postgres can not */
use plato;

$input = (select key, key || subkey as subkey, value from Input);

$total_count = (select count(1) from $input);

$filtered = (select * from $input where key in ("023", "037", "075"));

$filtered_cnt = (select count(1) from $filtered);

select $filtered_cnt / cast($total_count as Double) as cnt;
