use plato;

pragma directread;

$in = (select * from Input where a != 0);

select * from $in;

