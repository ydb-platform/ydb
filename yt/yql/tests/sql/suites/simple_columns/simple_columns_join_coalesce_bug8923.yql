/* syntax version 1 */
USE plato;

pragma SimpleColumns;
-- fails with CoalesceJoinKeysOnQualifiedAll
pragma DisableCoalesceJoinKeysOnQualifiedAll;

$foo = select 1 as key, 1 as value1;
$bar = select 1l as key, 2 as value2;
$baz = select 1l as key, 2 as value3;


select foo.* from $foo as foo 
join $bar as bar on cast(foo.key as Int32) = bar.key
join $baz as baz on bar.key = baz.key

