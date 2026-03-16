create procedure proc1()
begin
  declare handler for foo begin end;
  select 1;
end;

create procedure proc2()
begin
  select 1;
end;
