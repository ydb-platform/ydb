/* postgres can not */
select * from (
select 1,AsStruct(2 as foo),Just(AsStruct(3 as bar)),
Just(AsStruct(Just(4) as qwe))
)
flatten columns;
