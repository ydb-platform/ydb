select count(*) as cnt, max(a) as m from t1 group by b having count(*)=1 order by cnt, m;

