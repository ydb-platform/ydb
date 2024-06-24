--!syntax_pg
SELECT t1.* FROM plato."Input" t1 JOIN plato."Input" t2 ON (t1.id1=t2.id2);
SELECT t1.* FROM plato."Input" t1, plato."Input" t2 WHERE (t1.id1=t2.id2);

