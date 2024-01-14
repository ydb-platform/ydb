CREATE TABLE db.simple_table (number INT) ENGINE = Log();
INSERT INTO db.simple_table VALUES ((1)), ((2)), ((3));

CREATE TABLE db.join_table (id INT, data INT) ENGINE = Log();
INSERT INTO db.join_table VALUES (1, 10), (2, 20), (3, 30);
