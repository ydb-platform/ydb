/* syntax version 1 */
/* postgres can not */

$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "lax $.key" RETURNING Uint32 DEFAULT -2 ON EMPTY ERROR ON ERROR);
