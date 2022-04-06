select distinct b, a from (select a, b from t1 union all select b, a from t1) order by b, a;

