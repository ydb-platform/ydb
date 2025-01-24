/* syntax version 1 */
/* postgres can not */
-- not supported on windows

$input = (
	SELECT String::JoinFromList(AsList(key, subkey, value), ",") AS Data FROM plato.Input1
);

$processed = (
	PROCESS $input USING Streaming::Process(TableRows(), "grep", AsList("[14]"))
);

$list = (
	SELECT String::SplitToList(Data, ',') AS DataList FROM $processed
);

SELECT 
	input.DataList[0] AS key,
	input.DataList[1] AS subkey,
	input.DataList[2] AS value
FROM $list AS input;

