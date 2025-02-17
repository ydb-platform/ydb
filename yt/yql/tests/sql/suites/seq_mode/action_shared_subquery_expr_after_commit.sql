pragma SeqMode;
use plato;
define action $a() as 
    insert into @tmp
    select 1;
    commit;
    $r = select * from @tmp;
    select * from $r;
    select * from $r;
end define;

do $a();
