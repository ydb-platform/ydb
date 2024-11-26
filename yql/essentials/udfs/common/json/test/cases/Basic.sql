/* syntax version 0 */
$json1 =  @@{
    "x": {
        "y": ["15", "11", "17"],
        "z": 1
    }
}@@;

SELECT
    Json::GetField($json1, "/x/y/[1]"),
    Json::GetField("[]", "/"),
    Json::GetField($json1, "///");
