/* custom error:Empty result*/

-- Accessing absent object member will cause empty result in lax mode
$json = CAST("{}" as Json);
SELECT
    JSON_QUERY($json, "lax $.key" ERROR ON EMPTY);
