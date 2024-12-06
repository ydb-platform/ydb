PRAGMA dq.UseWideChannels = "false";
USE plato;

SELECT
    e.id AS id,
    e.ts AS ts,
    e.host AS host,
    h.fqdn AS fqdn,
    h.ip4 AS ip4,
    h.ip6 AS ip6
FROM
    Event AS e
LEFT JOIN
    /*+ streamlookup() */ Host AS h
ON
    (e.host == h.hostname)
;
