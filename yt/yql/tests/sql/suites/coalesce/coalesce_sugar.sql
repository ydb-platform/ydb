/* postgres can not */
$data = (select key, cast(key as Int64)/100 as eval from plato.Input);
select case when eval < 5 then eval else cast(Null as Int64) end ?? -1, key from $data;
