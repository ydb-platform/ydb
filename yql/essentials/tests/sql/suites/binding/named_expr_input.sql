/* syntax version 1 */
/* postgres can not */
$foo = (select 100500 as bar);
select bar from $foo;
