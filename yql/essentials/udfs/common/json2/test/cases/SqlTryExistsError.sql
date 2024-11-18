/* syntax version 1 */

$path = Json2::CompilePath("strict $.x");
$json = CAST("[]" as Json);

SELECT
    Json2::SqlTryExists($json, $path, AsDict());