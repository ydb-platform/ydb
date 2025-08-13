/* custom error:Member not found*/
-- Accessing absent object member will cause jsonpath error in strict mode
$json = CAST("{}" as Json);
SELECT
    JSON_QUERY($json, "strict $.key" ERROR ON ERROR);
