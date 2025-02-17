/* postgres can not */
USE plato;

$data = (
    SELECT
        Url::Normalize(value) AS normalized_url,
        Url::GetHost(Url::Normalize(value)) AS host,
        Url::GetDomain(Url::Normalize(value), 1) AS tld
    FROM CONCAT(
        `Input1`,
        `Input2`
    )
);
$ru_hosts = (
    SELECT
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
    host AS host,
    COUNT(*) AS hits_count
FROM $ru_hosts
GROUP BY host
ORDER BY hits_count DESC
LIMIT 25;
