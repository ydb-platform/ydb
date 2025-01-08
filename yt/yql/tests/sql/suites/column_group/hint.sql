USE plato;

$i1 = select * from Input where a > "a"; -- several publish consumers with same groups
$i2 = select * from Input where a > "a1"; -- several publish consumers with different groups
$i3 = select * from Input where a < "a2"; -- several consumers including publish
$i4 = select * from Input where a != "a"; -- several publish consumers with and without groups

-- test column group spec normalization
insert into Output1 with column_groups="{g1=[a;b;c];def=#}" select * from $i1;
insert into Output1 with column_groups="{def=#;g1=[c;a;b];}" select * from $i2;

insert into Output2 with column_groups="{def=#}" select * from $i2;
insert into Output2 with column_groups="{def=#}" select * from $i3;

insert into Output3 with column_groups="{g1=[a;b;c];def=#}" select * from $i1;
insert into Output3 with column_groups="{g1=[a;b;c];def=#}" select * from $i4;

insert into Output4 select * from $i4;

select a,b,c,d from $i3;
