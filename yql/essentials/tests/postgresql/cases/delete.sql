CREATE TABLE delete_test (
    id SERIAL PRIMARY KEY,
    a INT,
    b text
);
INSERT INTO delete_test (a) VALUES (10);
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));
INSERT INTO delete_test (a) VALUES (100);
DROP TABLE delete_test;
