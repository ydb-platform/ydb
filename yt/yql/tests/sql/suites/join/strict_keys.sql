/* custom error:Strict key type match requested, but keys have different types*/
use plato;

define subquery $strict() as
  pragma StrictJoinKeyTypes;
  select count(*) from Input1 as a join Input2 as b using(k1)
end define;

select count(*) from Input1 as a join Input2 as b using(k1);
select * from $strict();
