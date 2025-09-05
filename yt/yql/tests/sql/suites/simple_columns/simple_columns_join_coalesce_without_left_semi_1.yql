/* syntax version 1 */
USE plato;

pragma SimpleColumns;
pragma DisableCoalesceJoinKeysOnQualifiedAll;

select a.* without a.key, a.value from Input as a left semi join Input as b using(key) order by subkey;
select * without a.key, a.value from Input as a left semi join Input as b using(key) order by subkey;
