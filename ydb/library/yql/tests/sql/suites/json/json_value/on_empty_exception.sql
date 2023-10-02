/* syntax version 1 */
/* postgres can not */

$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "lax $.key" ERROR ON EMPTY);
