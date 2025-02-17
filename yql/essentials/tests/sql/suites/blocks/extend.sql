$data1 = [<|x:1|>,<|x:3|>,<|x:2|>];
$data2 = [<|x:4|>,<|x:1|>];

select * from as_table($data1)
union all
select * from as_table($data2)
