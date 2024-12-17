/* syntax version 1 */
$yson = cast('{"commands"=[{"command"="say";"text"="hello world"}]}' as yson);
SELECT Yson::ConvertToString($yson["command" || "s"].0["text"]) as text;

