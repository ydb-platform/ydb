/* postgres can not */
USE plato;

$data = (
    SELECT
        Url::Normalize(url) AS normalized_url,
        Url::GetHost(Url::Normalize(url)) AS host,
        Url::GetDomain(Url::Normalize(url), 1) AS tld
    FROM CONCAT(
        `Input1`,
        `Input2`
    )
);
$ru_hosts = (
    SELECT
        tld,
        host
    FROM
        $data
    WHERE normalized_url IS NOT NULL AND (
           tld = "ru"
        OR tld = "su"
        OR tld = "рф"
        OR tld = "xn--p1ai" -- punycode рф
    )
);

SELECT
    tld,
    COUNT(DISTINCT host) AS hosts_count
FROM $ru_hosts
GROUP BY tld
ORDER BY hosts_count DESC;
