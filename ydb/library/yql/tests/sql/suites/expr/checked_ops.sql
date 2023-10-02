pragma CheckedOps="true";
select 
5+3,
200ut+200ut,
18446744073709551615ul+18446744073709551615ul;
select 
5-3,
-120t-100t,
-9223372036854775807L-2l;
select
5*3,
200ut*200ut,
18446744073709551615ul*18446744073709551615ul;
select
5/3,
200ut/1t;
select
5%3,
100t%200ut;
select
-cast("-128" as int8);