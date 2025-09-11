/* syntax version 1 */
/* dqfile can not */
USE plato;

PRAGMA DqEngine="force";
PRAGMA dq.MemoryLimit="1M";
SELECT String::JoinFromList(ListMap(ListFromRange(0, 1000000), ($_) -> { return "0"; }), "");
