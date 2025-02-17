/* syntax version 1 */
/* postgres can not */
-- not supported on windows

$input = (
	SELECT String::JoinFromList(AsList(key, subkey, value), ",") AS Data FROM plato.Input1
);

$processed = (
	PROCESS $input USING Streaming::Process(TableRows(), "grep", AsList("[14]"))
);

SELECT 
    *
FROM $processed;

SELECT
    COUNT(*)
FROM $processed;
