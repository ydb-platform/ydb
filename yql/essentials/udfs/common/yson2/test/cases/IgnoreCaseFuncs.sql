pragma config.flags("UdfIgnoreCase");
select 
    YSON::PARSE('[]'),yson::parse('{}'y),yson::from(1),yson::getlength('[1;2;3]'y),
    yson::convertto("1"y,Int32);

