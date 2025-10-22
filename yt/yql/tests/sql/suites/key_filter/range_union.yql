/* postgres can not */
select * from (
    select * from plato.Input where key=8 or key>=9 --[8,)
    union all
    select * from plato.Input where key=8 or key>=8 --[8,)
    union all
    select * from plato.Input where key=8 or key>8  --[8,)
    union all
    select * from plato.Input where key=8 or key>7  --[8,)
    union all
    select * from plato.Input where key=8 or key>=7 --[7,)
    union all

    select * from plato.Input where key=8 or key<=7 --(,9)
    union all
    select * from plato.Input where key=8 or key<=8 --(,9)
    union all
    select * from plato.Input where key=8 or key<8  --(,9)
    union all
    select * from plato.Input where key=8 or key<9  --(,9)
    union all
    select * from plato.Input where key=8 or key<=9 --(,10)
    union all

    select * from plato.Input where key>8 or key<8 --(,8),[9,)
    union all
    select * from plato.Input where key>8 or key<=8 --(,)
    union all
    select * from plato.Input where key>8 or key<9 --(,)
    union all
    select * from plato.Input where key>7 or key<9 --(,)
    union all
    select * from plato.Input where key>=7 or key<9 --(,)
    union all
    select * from plato.Input where key>7 or key<=9 --(,)
    union all

    select * from plato.Input where (key>5 and key<7) or (key>4 and key<8) -- [5,8)
    union all
    select * from plato.Input where (key>5 and key<7) or (key>5 and key<8) -- [6,8)
    union all
    select * from plato.Input where (key>=5 and key<7) or (key>=5 and key<8) -- [5,8)
    union all
    select * from plato.Input where (key>5 and key<=7) or (key>4 and key<8) -- [5,8)
    union all
    select * from plato.Input where (key>5 and key<=7) or (key>4 and key<=7) -- [5,8)
    union all
    select * from plato.Input where (key>5 and key<8) or (key>4 and key<8) -- [5,8)
    union all

    select * from plato.Input where (key=1 and subkey<4 and subkey>1) or (key=2 and subkey<4 and subkey>1) --[{1,2},{1,4}),[{2,2},{2,4})
    union all
    select * from plato.Input where (key=1 and subkey<3 and subkey>1) or (key=2 and subkey<3 and subkey>1) --[{1,2}],[{2,2}]
    union all
    select * from plato.Input where (key=1 and subkey>1) or (key=2 and subkey<3) --[{1,2},{1}],[{2},{2,3})
    union all
    select * from plato.Input where (key=1 and subkey<3 and subkey>1) or key=1 --[1]
    union all
    select * from plato.Input where (key=1 and subkey>1) or key=1 --[1]
    union all
    select * from plato.Input where (key=1 and subkey<4 and subkey>1) or (key>1) --[{1,2},{1,4}),[2,)
    union all 
    select * from plato.Input where key=1 and (subkey between 2 and 3 or subkey < 2) --[{1},{1,3}]
    union all 
    select * from plato.Input where key=1 and (subkey >= 2 or subkey < 2) --[1]
)
order by key,subkey;
