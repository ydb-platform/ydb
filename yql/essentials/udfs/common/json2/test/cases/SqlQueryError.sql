/* syntax version 1 */

$path = Json2::CompilePath("strict $.x");

$jsonpath_error = CAST(@@{"y": []}@@ as Json);
SELECT
    Json2::SqlQuery($jsonpath_error, $path, AsDict(), false, NULL, true, NULL);