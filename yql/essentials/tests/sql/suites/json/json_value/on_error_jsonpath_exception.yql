/* custom error:Member not found*/

$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "strict $.key" ERROR ON ERROR);
