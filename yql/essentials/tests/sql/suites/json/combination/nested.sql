/* syntax version 1 */
/* postgres can not */

-- Check that Json2::Parse/Serialize calls fold
$json = CAST(@@{
    "a": {
        "b": {
            "c": {
                "d": "Matreshka!"
            }
        }
    }
}@@ as Json);

SELECT
    JSON_VALUE(
        JSON_QUERY(
            JSON_QUERY(
                JSON_QUERY(
                    $json,
                    "strict $.a"
                ),
                "strict $.b"
            ),
            "strict $.c"
        ),
        "strict $.d"
    );

SELECT
    JSON_EXISTS(
        JSON_QUERY(
            JSON_QUERY(
                JSON_QUERY(
                    $json,
                    "strict $.a"
                ),
                "strict $.b"
            ),
            "strict $.c"
        ),
        "strict $.d"
    );

SELECT
    JSON_QUERY(
        JSON_QUERY(
            JSON_QUERY(
                $json,
                "strict $.a"
            ),
            "strict $.b"
        ),
        "strict $.c"
    );