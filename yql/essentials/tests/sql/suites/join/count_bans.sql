PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$records = (
SELECT
    key as ip,
    subkey AS recordType,
    Url::GetHost(value) AS host
FROM spider_info
);

$results = (
SELECT
    ip,
    host,
    count(*) AS request_count
FROM $records
WHERE host IS NOT NULL AND recordType == "RESULT"
GROUP BY ip, host
);

$bans = (
SELECT
    ip,
    host,
    count(*) AS fetcher_count
FROM $records
WHERE host IS NOT NULL AND recordType == "BAN_DETECTED"
GROUP BY ip, host
);

SELECT
    results.ip AS ip,
    results.host AS host,
    results.request_count AS request_count,
    bans.fetcher_count AS fetcher_count
FROM
    $results AS results
    INNER JOIN
    $bans AS bans
    ON bans.ip == results.ip
    AND bans.host == results.host
ORDER BY fetcher_count DESC
;
