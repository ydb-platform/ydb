/* yt can not */
use plato;

evaluate parallel for $i in [1,2,1,2,1] do begin
insert into Output
select $i as a;
end do;

commit;
insert into Output with truncate
select a from Output order by a;
