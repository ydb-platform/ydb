select t1.a as A1, t1.b as B1, t2.a as A2, t2.b as B2 from t1 join t2 using (a) order by A1;
