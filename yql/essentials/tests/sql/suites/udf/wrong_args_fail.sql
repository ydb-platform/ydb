/* postgres can not */
/* custom error:Callable expected at most 3 argument(s)*/

-- Find has optional args
select String::ReplaceAll(); -- too few
select String::ReplaceAll("abc"); -- too few

select String::ReplaceAll("abc", "b", 2, 4); -- too many
select String::ReplaceAll("abc" , "b", 2, 4, 44); -- too many
