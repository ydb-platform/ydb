/* syntax version 1 */
/* postgres can not */
-- not supported on windows

$script = @@
#!/bin/bash
cat - | grep $1 | head -n 3 | grep [234]
@@;

$input = (
	SELECT String::JoinFromList(AsList(key, subkey, value), ",") AS Data FROM plato.Input1
);

PROCESS $input USING Streaming::ProcessInline(TableRows(), $script, AsList("bar"));
