/* custom error:Failed to cast default value from ON EMPTY clause to target type Optional<Uint32>*/

$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "lax $.key" RETURNING Uint32 DEFAULT -2 ON EMPTY ERROR ON ERROR);
