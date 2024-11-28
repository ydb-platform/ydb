/* syntax version 1 */
USE plato;

$logs_path = "//logs/antirobot-daemon-log2/1d";
$results_path = "//home/antispam/antirobot/sharding_daily";
$logs_per_run = 10;

DEFINE SUBQUERY $last_tables($path, $limit) AS
    SELECT AGGREGATE_LIST(Name)
    FROM (
        SELECT ListLast(String::SplitToList(Path, "/")) as Name
        FROM FOLDER($path)
        WHERE Type = "table"
        ORDER BY Name DESC
        LIMIT $limit
    )
END DEFINE;


$logs = (SELECT * FROM $last_tables($logs_path, $logs_per_run));
$processed_logs = (SELECT * FROM $last_tables($results_path, $logs_per_run));

SELECT SetDifference(ToSet($logs), ToSet($processed_logs))
