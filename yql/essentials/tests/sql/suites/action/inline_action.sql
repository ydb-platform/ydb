/* syntax version 1 */
/* postgres can not */
do begin 
    select 1;
end do;

evaluate if true do begin
    select 1;
end do
else do begin
    select 2;
end do;

evaluate for $i in AsList(1,2,3) do begin
    select $i;
end do;
