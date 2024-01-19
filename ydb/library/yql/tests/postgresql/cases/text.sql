--
-- TEXT
--
SELECT text 'this is a text string' = text 'this is a text string' AS true;
SELECT text 'this is a text string' = text 'this is a text strin' AS false;
CREATE TABLE TEXT_TBL (f1 text);
INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');
select reverse('abcde');
select i, left('ahoj', i), right('ahoj', i) from generate_series(-5, 5) t(i) order by i;
select quote_literal('');
select quote_literal('abc''');
select quote_literal(e'\\');
/*
 * format
 */
select format(NULL);
select format('Hello');
select format('Hello %%');
select format('Hello %%%%');
select format('Hello %s');
