USE hahn;

$minus_date = Python::minus_date(
    @@(String?,String?)->Int64@@,
    @@
from datetime import datetime, timedelta
def minus_date(date1, date2):
    return (datetime.strptime(date1, "%Y-%m-%d") - datetime.strptime(date2, "%Y-%m-%d")).days
    @@
);

$normalize_list = Python::normalize_list(
    @@(List<Tuple<Int64,Int64?>>)->List<Tuple<Int64,Int64>>@@,
    @@
def normalize_list(values):
    return sorted((x, y) for x, y in values if y)
    @@
);

$urls = (
FROM hits
SELECT url AS Url, "2016-10-25" AS Date
GROUP BY url
);

$flatten_factors = (
FROM $urls AS target
INNER JOIN hits AS history
ON target.Url == history.url
SELECT
    target.Url AS Url,
    target.Date AS Date,
    history.*
);

INSERT INTO [Out1] WITH TRUNCATE FROM $flatten_factors SELECT *;
COMMIT;

$pool = (
FROM [Out1]
SELECT
    Url,
    Date,
    Url::GetHost(Url) AS Host
/*    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_all_total]))) AS metrika_all_total
/*    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.yabro_mobile_unique]))) AS yabro_mobile_unique,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_counter_count]))) AS metrika_counter_count,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_yabro_desktop_total]))) AS metrika_yabro_desktop_total,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_yabro_desktop_unique]))) AS metrika_yabro_desktop_unique,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_all_unique]))) AS metrika_all_unique,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_yabro_mobile_unique]))) AS metrika_yabro_mobile_unique,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.yabro_desktop_total]))) AS yabro_desktop_total,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.yabro_desktop_unique]))) AS yabro_desktop_unique,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.yabro_mobile_total]))) AS yabro_mobile_total,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.metrika_yabro_mobile_total]))) AS metrika_yabro_mobile_total,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.cs_clicks]))) AS cs_clicks,
    $normalize_list(LIST(($minus_date(Date, [history.date]), [history.cs_shows]))) AS cs_shows*/
GROUP BY Url, Date
);

$hosts = (
FROM $pool
SELECT Host, "2016-10-25" AS Date
GROUP BY Host
);

$host_features = (
FROM $hosts AS hosts
LEFT JOIN hits AS all_hits
ON hosts.Host == Url::GetHost(all_hits.url)
SELECT
    hosts.Host AS Host,
    hosts.Date AS Date,
/*    SUM(all_hits.metrika_all_total) AS metrika_all_total_sum7,
/*    MAX(all_hits.metrika_all_total) AS metrika_all_total_max7,
--     STDDEV(all_hits.metrika_all_total) AS metrika_all_total_stddev7,
    MEDIAN(all_hits.metrika_all_total) AS metrika_all_total_median7,
    PERCENTILE(all_hits.metrika_all_total, 0.25) AS metrika_all_total_25perc7,
    PERCENTILE(all_hits.metrika_all_total, 0.75) AS metrika_all_total_75perc7,
    COUNT_IF(all_hits.metrika_all_total > 0) AS metrika_all_total_nonzero7,
    SUM(all_hits.yabro_mobile_unique) AS yabro_mobile_unique_sum7,
    MAX(all_hits.yabro_mobile_unique) AS yabro_mobile_unique_max7,
--     STDDEV(all_hits.yabro_mobile_unique) AS yabro_mobile_unique_stddev7,
    MEDIAN(all_hits.yabro_mobile_unique) AS yabro_mobile_unique_median7,
    PERCENTILE(all_hits.yabro_mobile_unique, 0.25) AS yabro_mobile_unique_25perc7,
    PERCENTILE(all_hits.yabro_mobile_unique, 0.75) AS yabro_mobile_unique_75perc7,
    COUNT_IF(all_hits.yabro_mobile_unique > 0) AS yabro_mobile_unique_nonzero7,
    SUM(all_hits.metrika_counter_count) AS metrika_counter_count_sum7,
    MAX(all_hits.metrika_counter_count) AS metrika_counter_count_max7,
--     STDDEV(all_hits.metrika_counter_count) AS metrika_counter_count_stddev7,
    MEDIAN(all_hits.metrika_counter_count) AS metrika_counter_count_median7,
    PERCENTILE(all_hits.metrika_counter_count, 0.25) AS metrika_counter_count_25perc7,
    PERCENTILE(all_hits.metrika_counter_count, 0.75) AS metrika_counter_count_75perc7,
    COUNT_IF(all_hits.metrika_counter_count > 0) AS metrika_counter_count_nonzero7,
    SUM(all_hits.metrika_yabro_desktop_total) AS metrika_yabro_desktop_total_sum7,
    MAX(all_hits.metrika_yabro_desktop_total) AS metrika_yabro_desktop_total_max7,
--     STDDEV(all_hits.metrika_yabro_desktop_total) AS metrika_yabro_desktop_total_stddev7,
    MEDIAN(all_hits.metrika_yabro_desktop_total) AS metrika_yabro_desktop_total_median7,
    PERCENTILE(all_hits.metrika_yabro_desktop_total, 0.25) AS metrika_yabro_desktop_total_25perc7,
    PERCENTILE(all_hits.metrika_yabro_desktop_total, 0.75) AS metrika_yabro_desktop_total_75perc7,
    COUNT_IF(all_hits.metrika_yabro_desktop_total > 0) AS metrika_yabro_desktop_total_nonzero7,
    SUM(all_hits.metrika_yabro_desktop_unique) AS metrika_yabro_desktop_unique_sum7,
    MAX(all_hits.metrika_yabro_desktop_unique) AS metrika_yabro_desktop_unique_max7,
--     STDDEV(all_hits.metrika_yabro_desktop_unique) AS metrika_yabro_desktop_unique_stddev7,
    MEDIAN(all_hits.metrika_yabro_desktop_unique) AS metrika_yabro_desktop_unique_median7,
    PERCENTILE(all_hits.metrika_yabro_desktop_unique, 0.25) AS metrika_yabro_desktop_unique_25perc7,
    PERCENTILE(all_hits.metrika_yabro_desktop_unique, 0.75) AS metrika_yabro_desktop_unique_75perc7,
    COUNT_IF(all_hits.metrika_yabro_desktop_unique > 0) AS metrika_yabro_desktop_unique_nonzero7,
    SUM(all_hits.metrika_all_unique) AS metrika_all_unique_sum7,
    MAX(all_hits.metrika_all_unique) AS metrika_all_unique_max7,
--     STDDEV(all_hits.metrika_all_unique) AS metrika_all_unique_stddev7,
    MEDIAN(all_hits.metrika_all_unique) AS metrika_all_unique_median7,
    PERCENTILE(all_hits.metrika_all_unique, 0.25) AS metrika_all_unique_25perc7,
    PERCENTILE(all_hits.metrika_all_unique, 0.75) AS metrika_all_unique_75perc7,
    COUNT_IF(all_hits.metrika_all_unique > 0) AS metrika_all_unique_nonzero7,
    SUM(all_hits.metrika_yabro_mobile_unique) AS metrika_yabro_mobile_unique_sum7,
    MAX(all_hits.metrika_yabro_mobile_unique) AS metrika_yabro_mobile_unique_max7,
--     STDDEV(all_hits.metrika_yabro_mobile_unique) AS metrika_yabro_mobile_unique_stddev7,
    MEDIAN(all_hits.metrika_yabro_mobile_unique) AS metrika_yabro_mobile_unique_median7,
    PERCENTILE(all_hits.metrika_yabro_mobile_unique, 0.25) AS metrika_yabro_mobile_unique_25perc7,
    PERCENTILE(all_hits.metrika_yabro_mobile_unique, 0.75) AS metrika_yabro_mobile_unique_75perc7,
    COUNT_IF(all_hits.metrika_yabro_mobile_unique > 0) AS metrika_yabro_mobile_unique_nonzero7,
    SUM(all_hits.yabro_desktop_total) AS yabro_desktop_total_sum7,
    MAX(all_hits.yabro_desktop_total) AS yabro_desktop_total_max7,
--     STDDEV(all_hits.yabro_desktop_total) AS yabro_desktop_total_stddev7,
    MEDIAN(all_hits.yabro_desktop_total) AS yabro_desktop_total_median7,
    PERCENTILE(all_hits.yabro_desktop_total, 0.25) AS yabro_desktop_total_25perc7,
    PERCENTILE(all_hits.yabro_desktop_total, 0.75) AS yabro_desktop_total_75perc7,
    COUNT_IF(all_hits.yabro_desktop_total > 0) AS yabro_desktop_total_nonzero7,
    SUM(all_hits.yabro_desktop_unique) AS yabro_desktop_unique_sum7,
    MAX(all_hits.yabro_desktop_unique) AS yabro_desktop_unique_max7,
--     STDDEV(all_hits.yabro_desktop_unique) AS yabro_desktop_unique_stddev7,
    MEDIAN(all_hits.yabro_desktop_unique) AS yabro_desktop_unique_median7,
    PERCENTILE(all_hits.yabro_desktop_unique, 0.25) AS yabro_desktop_unique_25perc7,
    PERCENTILE(all_hits.yabro_desktop_unique, 0.75) AS yabro_desktop_unique_75perc7,
    COUNT_IF(all_hits.yabro_desktop_unique > 0) AS yabro_desktop_unique_nonzero7,
    SUM(all_hits.yabro_mobile_total) AS yabro_mobile_total_sum7,
    MAX(all_hits.yabro_mobile_total) AS yabro_mobile_total_max7,
--     STDDEV(all_hits.yabro_mobile_total) AS yabro_mobile_total_stddev7,
    MEDIAN(all_hits.yabro_mobile_total) AS yabro_mobile_total_median7,
    PERCENTILE(all_hits.yabro_mobile_total, 0.25) AS yabro_mobile_total_25perc7,
    PERCENTILE(all_hits.yabro_mobile_total, 0.75) AS yabro_mobile_total_75perc7,
    COUNT_IF(all_hits.yabro_mobile_total > 0) AS yabro_mobile_total_nonzero7,
    SUM(all_hits.metrika_yabro_mobile_total) AS metrika_yabro_mobile_total_sum7,
    MAX(all_hits.metrika_yabro_mobile_total) AS metrika_yabro_mobile_total_max7,
--     STDDEV(all_hits.metrika_yabro_mobile_total) AS metrika_yabro_mobile_total_stddev7,
    MEDIAN(all_hits.metrika_yabro_mobile_total) AS metrika_yabro_mobile_total_median7,
    PERCENTILE(all_hits.metrika_yabro_mobile_total, 0.25) AS metrika_yabro_mobile_total_25perc7,
    PERCENTILE(all_hits.metrika_yabro_mobile_total, 0.75) AS metrika_yabro_mobile_total_75perc7,
    COUNT_IF(all_hits.metrika_yabro_mobile_total > 0) AS metrika_yabro_mobile_total_nonzero7,*/
    COUNT(*) AS url_day_count,
    COUNT(DISTINCT(all_hits.url)) AS url_count
GROUP BY hosts.Host, hosts.Date
);

INSERT INTO [Out2] WITH TRUNCATE
FROM $pool AS pool
LEFT JOIN $host_features AS host
ON Url::GetHost(pool.Url) == host.Host AND pool.Date == host.Date
SELECT *;
/*    pool.Date AS Date,
    pool.yabro_mobile_unique AS yabro_mobile_unique,
    pool.Url AS Url,
    pool.cs_clicks AS cs_clicks,
    pool.cs_shows AS cs_shows,
    pool.metrika_all_total AS metrika_all_total,
    pool.metrika_all_unique AS metrika_all_unique,
    pool.Host AS Host,
    pool.metrika_yabro_desktop_total AS metrika_yabro_desktop_total,
    pool.metrika_yabro_desktop_unique AS metrika_yabro_desktop_unique,
    pool.metrika_yabro_mobile_total AS metrika_yabro_mobile_total,
    pool.metrika_yabro_mobile_unique AS metrika_yabro_mobile_unique,
    pool.yabro_desktop_total AS yabro_desktop_total,
    pool.yabro_desktop_unique AS yabro_desktop_unique,
    pool.yabro_mobile_total AS yabro_mobile_total,
    pool.metrika_counter_count AS metrika_counter_count,
    host.*;*/

COMMIT;
DROP TABLE [Out1];

