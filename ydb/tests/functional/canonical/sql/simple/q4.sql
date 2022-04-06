select count(*) as cnt, max(a) as max from t1 group by b order by cnt, max;

