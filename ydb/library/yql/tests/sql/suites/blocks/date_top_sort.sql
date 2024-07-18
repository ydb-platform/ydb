USE plato;
pragma yt.UseNativeDescSort;

SELECT
    t.*, i8+1
FROM (SELECT * FROM concat(Dates,Dates)) as t
ORDER BY na,wa,naz,waz,nd,ndz,wd,wdz,nt,ntz,wt,wtz,ni,wi
LIMIT 1;

