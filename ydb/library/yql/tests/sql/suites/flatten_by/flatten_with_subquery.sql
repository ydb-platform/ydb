/* syntax version 1 */
/* postgres can not */

define subquery $bar() as
    select [1,2] as ks; 
end define;

select key from $bar() flatten list by ks as key order by key;
select key from $bar() flatten list by (ListExtend(ks, [3]) as key) order by key;
