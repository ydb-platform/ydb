USE plato;
pragma yt.UseNativeDescSort;

SELECT
    t.*, i8+1
FROM (SELECT * FROM concat(Dates,Dates)) as t
ORDER BY na,wa,naz,nd,ndz,wd,nt,ntz,wt,ni,wi
LIMIT 1;

