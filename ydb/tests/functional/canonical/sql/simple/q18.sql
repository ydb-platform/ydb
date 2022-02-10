select t.a as co, b from t1 join (select t1.a as a, t3.c as c
from t1 join t3 on t1.a = t3.a)  as t on t.a = t1.a order by co;
