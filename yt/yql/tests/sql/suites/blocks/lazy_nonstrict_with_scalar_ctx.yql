USE plato;

$one = select min(AssumeNonStrict(value)) from Input;
$two = select AssumeNonStrict(min(value)) from Input;

-- fully converted to blocks - scalar context is assumed strict
select * from Input where subkey != "1" and value > $one;

-- partially converted to blocks - AssumeStrict is calculated outside of scalar context
select * from Input where subkey != "2" and value > $two;
