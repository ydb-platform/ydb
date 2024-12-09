/* syntax version 1 */
$l = AsList(
    AsStruct(AsTuple(1,2,3) as Key, "1,2,3" as Lhs),
    AsStruct(AsTuple(1,2,4) as Key, "1,2,4" as Lhs)
);

$r = AsList(
    AsStruct(AsTuple(1,2) as Key, "1,2" as Rhs),
    AsStruct(AsTuple(2,3) as Key, "2,3" as Rhs)
);

select Lhs, Rhs from AS_TABLE($l) as l left join AS_TABLE($r) as r using(Key);
select Lhs from AS_TABLE($l) as l left semi join AS_TABLE($r) as r using(Key);
select Lhs from AS_TABLE($l) as l left only join AS_TABLE($r) as r using(Key);

select Rhs, Lhs from AS_TABLE($l) as l right join AS_TABLE($r) as r using(Key);
select Rhs from AS_TABLE($l) as l right semi join AS_TABLE($r) as r using(Key);
select Rhs from AS_TABLE($l) as l right only join AS_TABLE($r) as r using(Key);

select Lhs, Rhs from AS_TABLE($l) as l inner join AS_TABLE($r) as r using(Key);
select Lhs, Rhs from AS_TABLE($l) as l full join AS_TABLE($r) as r using(Key);
select Lhs, Rhs from AS_TABLE($l) as l exclusion join AS_TABLE($r) as r using(Key);
