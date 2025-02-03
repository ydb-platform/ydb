--!syntax_pg

select
-- case $x when
case 2 when 1 then 777 else 999 end a1,
case 1 when 1 then 777 else 999 end a2,
case 1 when 1 then 3 end a3,
case 1 when 2 then 3 end a4,
case 1 when 2 then 3 else 4 end a5,
case 1 when 2 then 20 when 3 then 30 when 4 then 40 when 5 then 50 when 6 then 60 when 7 then 70 else 100 end a6,
case 1 when 2 then 20 when 3 then 30 when 4 then 40 when 5 then 50 when 6 then 60 when 7 then 70 end a7,

-- case when
case when 2=1 then 777 else 999 end b1,
case when 1=1 then 777 else 999 end b2,
case when 1=1 then 3 end b3,
case when 1=2 then 3 end b4,
case when 1=2 then 3 else 4 end b5,
case when 1=2 then 20 when 1=3 then 30 when 1=4 then 40 when 1=5 then 50 when 1=6 then 60 when 1=7 then 70 else 100 end b6,
case when 1=2 then 20 when 1=3 then 30 when 1=4 then 40 when 1=5 then 50 when 1=6 then 60 when 1=7 then 70 end b7
