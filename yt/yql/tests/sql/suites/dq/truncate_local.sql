pragma DqEngine="auto";
select * from AS_TABLE(ListMap(ListFromRange(1,10000),($x)->(<|a:$x|>)));

