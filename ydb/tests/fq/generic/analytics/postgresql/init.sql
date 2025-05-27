CREATE TABLE simple_table (number INT);
INSERT INTO simple_table VALUES ((1)), ((2)), ((3));

CREATE TABLE join_table (id INT, data bytea);
INSERT INTO join_table VALUES (1, 'pg10'), (2, 'pg20'), (3, 'pg30');
