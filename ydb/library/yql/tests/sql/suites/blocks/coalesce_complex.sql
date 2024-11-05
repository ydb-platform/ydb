USE plato;

$strings = [
   <|key:Just(Just("foo")),      subkey:Just("bar"),      value:1|>,
   <|key:Just(Nothing(String?)), subkey:Just("two"),      value:2|>,
   <|key:Nothing(String??),      subkey:Just("three"),    value:3|>,
   <|key:Nothing(String??),      subkey:Nothing(String?), value:4|>,
];

$tuples = [
   <|key:Just(Just(AsTuple(1, 2))),       subkey:Just(AsTuple(3, 4)),       value:1|>,
   <|key:Just(Nothing(Tuple<Int, Int>?)), subkey:Just(AsTuple(4, 5)),       value:2|>,
   <|key:Nothing(Tuple<Int, Int>??),      subkey:Just(AsTuple(5, 6)),       value:3|>,
   <|key:Nothing(Tuple<Int, Int>??),      subkey:Nothing(Tuple<Int, Int>?), value:4|>,
];

insert into @strings
select * from as_table($strings);

insert into @tuples
select * from as_table($tuples);

commit;

select
   value,
   key ?? subkey,
   subkey ?? "xxx",
from @strings
order by value;

select
   value,
   key ?? subkey,
   subkey ?? AsTuple(100, 500),
from @tuples
order by value;
