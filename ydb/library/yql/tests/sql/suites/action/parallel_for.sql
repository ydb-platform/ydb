use plato;

evaluate parallel for $i in [5,2,7] do begin
insert into Output
select $i as a;
end do;

commit;
insert into Output with truncate
select a from Output order by a;
