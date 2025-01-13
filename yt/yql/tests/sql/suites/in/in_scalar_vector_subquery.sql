/* postgres can not */

use plato;

$bar = (select "1" union all select "2");
$barr = (select "1" as subkey union all select "2" as subkey);

select "1" in $bar, "2" in $bar;
select "3" in $bar;

select "1" in AsList($barr), "2" in AsList($barr);
select "3" in AsList($barr);

select * from Input where subkey in $bar order by subkey;
select * from Input where subkey in AsList($barr) order by subkey;

-- same content as $bar
$baz = (select subkey from Input where subkey == "1" or subkey == "2");
$bazz = (select subkey from Input where subkey < "3" order by subkey asc limit 1);

select "1" in $baz, "2" in $baz;
select "3" in $baz;

select "1" in AsList($bazz), "2" in AsList($bazz);
select "3" in AsList($bazz);

select * from Input where subkey in $baz order by subkey;
select * from Input where subkey in AsList($bazz) order by subkey;

