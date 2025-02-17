/* syntax version 1 */
$in = (SELECT value AS Data FROM Input);
PROCESS $in USING Streaming::Process(TableRows(), "tail", AsList("-n+101"));