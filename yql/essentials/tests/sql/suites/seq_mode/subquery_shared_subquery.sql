pragma SeqMode;

define subquery $a() as 
    $r = select 1 as x;
    select * from $r;
end define;

process $a();

