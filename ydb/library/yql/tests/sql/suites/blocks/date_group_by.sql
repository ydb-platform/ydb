USE plato;
pragma yt.UseNativeDescSort;
pragma yt.UsePartitionsByKeysForFinalAgg="false";

SELECT
    count(*),min(i8)
FROM (SELECT * FROM concat(Dates,Dates)) as t
GROUP BY na,wa,naz,waz,nd,ndz,wd,wdz,nt,ntz,wt,wtz,ni,wi;

