$data = [<|x:1|>,<|x:3|>,<|x:2|>];

select * from as_table($data) order by x;
select * from as_table($data) order by x desc limit 2;
