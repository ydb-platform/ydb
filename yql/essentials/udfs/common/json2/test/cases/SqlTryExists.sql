/* syntax version 1 */

$path = Json2::CompilePath("strict $.x");

-- Key exists
SELECT
    Json2::SqlTryExists(NULL, $path, AsDict()),
    Json2::SqlTryExists(CAST(@@{"x": 123}@@ as Json), $path, AsDict()),
    Json2::SqlTryExists(CAST(@@{"x": {"key": "value"}}@@ as Json), $path, AsDict()),
    Json2::SqlTryExists(CAST(@@{"x": [1, 2, 3]}@@ as Json), $path, AsDict()),
    Json2::SqlTryExists(CAST(@@{"x": null}@@ as Json), $path, AsDict());
