--
-- INT4
--
CREATE TABLE INT4_TBL(f1 int4 primary key);
INSERT INTO INT4_TBL(f1) VALUES ('   0  ');
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');
INSERT INTO INT4_TBL(f1) VALUES ('34.5');
-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');
-- bad input values -- should give errors
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
INSERT INTO INT4_TBL(f1) VALUES ('     ');
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
INSERT INTO INT4_TBL(f1) VALUES ('');
SELECT * FROM INT4_TBL;
SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int4 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 = int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 = int4 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 < int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 < int4 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int4 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 > int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 > int4 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int4 '0';
-- positive odds
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';
-- any evens
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';
SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;
SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;
SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;
SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;
SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;
SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;
SELECT i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;
SELECT i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;
