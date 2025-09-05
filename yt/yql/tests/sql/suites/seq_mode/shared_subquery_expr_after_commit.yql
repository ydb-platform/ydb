use plato;
pragma SeqMode;
insert into @foo
select 1;
commit;
$a = select * from @foo;
select * from $a;
select * from $a;

