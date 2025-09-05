/* syntax version 1 */
USE plato;

pragma SimpleColumns;
pragma CoalesceJoinKeysOnQualifiedAll;

$foo = select 1 as key, 1 as value1;
$bar = select 1l as key, 2 as value2;

select * from $foo as foo 
join $bar as bar on foo.key = bar.key;

-- output key has type Int64

