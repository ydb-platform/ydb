USE plato;

select
    key,
    b1 and b2   as abb,
    b2 and ob1  as abo,
    ob2 and b1  as aob,
    ob1 and ob2 as aoo,

    b1 or b2   as obb,
    b2 or ob1  as obo,
    ob2 or b1  as oob,
    ob1 or ob2 as ooo,

    b1 xor b2   as xbb,
    b2 xor ob1  as xbo,
    ob2 xor b1  as xob,
    ob1 xor ob2 as xoo,

    (1 > 2) and b1 and b2 and ob1 and ob2 as chain1,
    (1 < 2) xor b1 xor b2 xor ob1 xor ob2 as chain2,
    (1/0 < 1) or b1 or b2 or ob1 or ob2 as chain3,


from Input

order by key;
