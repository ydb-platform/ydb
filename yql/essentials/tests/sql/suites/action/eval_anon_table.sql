/* syntax version 1 */
/* postgres can not */
USE plato;

$a = CAST(Unicode::ToUpper("T"u) AS String) || "able";
$b = CAST(Unicode::ToUpper("T"u) AS String) || "able";

insert into @$a
select 1 as x;

commit;

select * from @$b;
