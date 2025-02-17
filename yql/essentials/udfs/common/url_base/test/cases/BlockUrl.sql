/* syntax version 1 */
PRAGMA UseBlocks;
SELECT
    value,
    Url::Encode(value) AS encode,
    Url::Decode(value) AS decode,
    Url::GetCGIParam(value, "foo") AS param,
    Url::CutQueryStringAndFragment(value) AS cut_qs_and_fragment,
    Url::GetHost(value) as host,
    Url::CutWWW(Url::GetHost(value)) AS cut_www,
    Url::CutWWW2(Url::GetHost(value)) AS cut_www2,
    Url::GetTLD(value) AS tld,
    Url::PunycodeToHostName(value) AS punycode,
    Url::CutScheme(value) AS cut_scheme,
    Url::GetHostPort(value) as host_port,
    Url::GetSchemeHost(value) AS scheme_host,
    Url::GetSchemeHostPort(value) AS scheme_host_port,
    Url::GetTail(value) AS tail,
    Url::GetPath(value) AS path,
    Url::GetFragment(value) AS fragment,
    Url::GetPort(value) AS port,
    Url::GetDomain(value, 0) as domain0,
    Url::GetDomain(value, 1) as domain1,
    Url::GetDomain(value, 3) as domain3,
    Url::GetDomainLevel(value) as domain_level,
    Url::Normalize(value) as norm
FROM Input;
