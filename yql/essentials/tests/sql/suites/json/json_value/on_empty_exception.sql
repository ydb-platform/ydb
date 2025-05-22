/* custom error:Result is empty*/

$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "lax $.key" ERROR ON EMPTY);
