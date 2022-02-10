select distinct a, b, x from (
 select a, b, yql::Concat(a, b) as x from t1) order by a;
