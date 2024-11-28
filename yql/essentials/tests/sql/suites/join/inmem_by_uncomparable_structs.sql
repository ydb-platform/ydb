/* syntax version 1 */
$l = [
    <|Key:<|a:1, b:2, c:null|>, Lhs:"1,2,#"|>,
    <|Key:<|a:2, b:3, c:null|>, Lhs:"4,5,#"|>,
];

$r = [
    <|Key:<|a:1, b:2, c:3|>, Rhs:"1,2,3"|>,
    <|Key:<|a:4, b:5, c:6|>, Rhs:"4,5,6"|>,
];

select Lhs, Rhs from AS_TABLE($l) as l left join AS_TABLE($r) as r using(Key);
select Lhs from AS_TABLE($l) as l left semi join AS_TABLE($r) as r using(Key);
select Lhs from AS_TABLE($l) as l left only join AS_TABLE($r) as r using(Key);

select Rhs, Lhs from AS_TABLE($l) as l right join AS_TABLE($r) as r using(Key);
select Rhs from AS_TABLE($l) as l right semi join AS_TABLE($r) as r using(Key);
select Rhs from AS_TABLE($l) as l right only join AS_TABLE($r) as r using(Key);

select Lhs, Rhs from AS_TABLE($l) as l inner join AS_TABLE($r) as r using(Key);
select Lhs, Rhs from AS_TABLE($l) as l full join AS_TABLE($r) as r using(Key);
select Lhs, Rhs from AS_TABLE($l) as l exclusion join AS_TABLE($r) as r using(Key);

