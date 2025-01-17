pragma CheckedOps="true";
select sum(null),sum(x),sumif(x,true),sum(just(x)),sumif(just(x),true) 
from (values (18446744073709551615ul),(2ul)) as a(x);
