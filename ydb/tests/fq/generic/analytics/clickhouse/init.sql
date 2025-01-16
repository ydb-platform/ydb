CREATE TABLE db.simple_table (number INT) ENGINE = Log();
INSERT INTO db.simple_table VALUES ((1)), ((2)), ((3));

CREATE TABLE db.join_table (id INT, data String) ENGINE = Log();
INSERT INTO db.join_table VALUES (1, 'ch10'), (2, 'ch20'), (3, 'ch30');
