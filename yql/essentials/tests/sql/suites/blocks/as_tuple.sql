$data = [<|x:1,y:'foo'|>];

select (x,1),(x,y) from as_table($data);
