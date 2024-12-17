USE plato;

PRAGMA yt.UseNativeDescSort;
PRAGMA yt.UsePartitionsByKeysForFinalAgg = 'false';

SELECT
    count(*),
    min(i8)
FROM (
    SELECT
        *
    FROM
        concat(Dates, Dates)
) AS t
GROUP BY
    na,
    wa,
    naz,
    waz,
    nd,
    ndz,
    wd,
    wdz,
    nt,
    ntz,
    wt,
    wtz,
    ni,
    wi
;
