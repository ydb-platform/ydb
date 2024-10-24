pragma dq.UseWideChannels = "true";
use plato;

select e.id as id, e.ts as ts, e.host as host, h.fqdn as fqdn, h.ip4 as ip4, h.ip6 as ip6
    from Event as e 
    left join /*+ streamlookup(TTL 1 MaxCachedRows 5 MaxDelayedRows 3) */ any Host as h
    on (e.host == h.hostname)
;

