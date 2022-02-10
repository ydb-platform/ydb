select count(*), max(a) from t1 group by b having count(*)=2;

