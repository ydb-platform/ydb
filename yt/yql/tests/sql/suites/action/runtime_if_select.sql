/* syntax version 1 */
/* postgres can not */
use plato;

insert into @foo
select count(*) as count from Input;
commit;
$n = (select count from @foo);
$predicate = $n > 1;

if $predicate do begin
    select 1;
end do;

if not $predicate do begin
    select 2;
end do;

if $predicate do begin
    select 3;
end do else do begin
    select 4;
end do;

if not $predicate do begin
    select 5;
end do else do begin
    select 6;
end do;
