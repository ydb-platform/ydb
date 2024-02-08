--
-- VARCHAR
--

CREATE TABLE VARCHAR_TBL(f1 varchar(1));

INSERT INTO VARCHAR_TBL (f1) VALUES ('a');

INSERT INTO VARCHAR_TBL (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO VARCHAR_TBL (f1) VALUES ('1');

INSERT INTO VARCHAR_TBL (f1) VALUES (2);

INSERT INTO VARCHAR_TBL (f1) VALUES ('3');

-- zero-length char
INSERT INTO VARCHAR_TBL (f1) VALUES ('');

-- try varchar's of greater than 1 length
INSERT INTO VARCHAR_TBL (f1) VALUES ('cd');
INSERT INTO VARCHAR_TBL (f1) VALUES ('c     ');


SELECT * FROM VARCHAR_TBL;

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <> 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 = 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 < 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <= 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 > 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 >= 'a';

DROP TABLE VARCHAR_TBL;

--
-- Now test longer arrays of char
--

CREATE TABLE VARCHAR_TBL(f1 varchar(4));

INSERT INTO VARCHAR_TBL (f1) VALUES ('a');
INSERT INTO VARCHAR_TBL (f1) VALUES ('ab');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcde');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd    ');

SELECT * FROM VARCHAR_TBL;
