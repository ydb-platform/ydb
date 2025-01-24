CREATE TABLE stud_emp (
	name 		text,
	age		int4,
	--location 	point,
	salary 		int4,
	manager 	name,
	gpa 		float8,
	percent 	int4
);

INSERT INTO stud_emp VALUES
('jeff',	23,	/*(8,7.7),*/	600,	'sharon',	3.50000000000000000e+00, null),
('cim',		30,	/*(10.5,4.7),*/	400,	null,		3.39999999999999990e+00, null),
('linda',	19,	/*(0.9,6.1),*/	100,	null,		2.89999999999999990e+00, null);

