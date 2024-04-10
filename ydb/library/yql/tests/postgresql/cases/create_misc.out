--
-- CREATE_MISC
--
-- CLASS POPULATION
--	(any resemblance to real life is purely coincidental)
--
INSERT INTO tenk2 SELECT * FROM tenk1;
INSERT INTO hobbies_r (name) VALUES ('skywalking');
INSERT INTO equipment_r (name, hobby) VALUES ('advil', 'posthacking');
INSERT INTO equipment_r (name, hobby) VALUES ('peet''s coffee', 'posthacking');
INSERT INTO equipment_r (name, hobby) VALUES ('hightops', 'basketball');
INSERT INTO equipment_r (name, hobby) VALUES ('guts', 'skywalking');
--
-- for internal portal (cursor) tests
--
CREATE TABLE iportaltest (
	i		int4,
	d		float4,
	p		polygon
);
INSERT INTO iportaltest (i, d, p)
   VALUES (1, 3.567, '(3.0,1.0),(4.0,2.0)'::polygon);
INSERT INTO iportaltest (i, d, p)
   VALUES (2, 89.05, '(4.0,2.0),(3.0,1.0)'::polygon);
