CREATE TABLE TEXT_TBL (f1 text);

INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

CREATE TABLE text_tbl (f1 text);
INSERT INTO text_tbl
SELECT * from TEXT_TBL;

