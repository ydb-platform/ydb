/* syntax version 1 */

$path = Json2::CompilePath("strict $.x");

-- Key exists
SELECT
    Json2::SqlExists(CAST(@@{"x": 123}@@ as Json), $path, AsDict(), false),
    Json2::SqlExists(CAST(@@{"x": {"key": "value"}}@@ as Json), $path, AsDict(), false),
    Json2::SqlExists(CAST(@@{"x": [1, 2, 3]}@@ as Json), $path, AsDict(), false),
    Json2::SqlExists(CAST(@@{"x": null}@@ as Json), $path, AsDict(), false);

-- Key is missing
SELECT
    Json2::SqlExists(NULL, $path, AsDict(), false),
    Json2::SqlExists(CAST(@@{"not_x": 123}@@ as Json), $path, AsDict(), false),
    Json2::SqlExists(CAST("{}" as Json), $path, AsDict(), false),
    Json2::SqlExists(CAST("[]" as Json), $path, AsDict(), false);

-- Error handling
$json = CAST("[]" as Json);

SELECT
    Json2::SqlExists($json, $path, AsDict(), false),
    Json2::SqlExists($json, $path, AsDict(), true),
    Json2::SqlExists($json, $path, AsDict(), NULL);