USE plato;
pragma yt.UseNativeDescSort;
pragma yt.UsePartitionsByKeysForFinalAgg="false";

SELECT
    count(*),min(i8)
FROM (SELECT * FROM concat(Dates,Dates)) as t
GROUP BY na,wa,naz,nd,ndz,wd,nt,ntz,wt,ni,wi;

