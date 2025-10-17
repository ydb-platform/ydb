--!syntax_pg
select * from json_each('{"a":"foo", "b":"bar"}');
select * from json_each('{"a":"foo", "b":"bar"}') as f;
select * from json_each('{"a":"foo", "b":"bar"}') as f(x,y);
