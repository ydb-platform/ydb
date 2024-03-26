--
-- TEXT
--
SELECT text 'this is a text string' = text 'this is a text string' AS true;
SELECT text 'this is a text string' = text 'this is a text strin' AS false;
CREATE TABLE TEXT_TBL (f1 text);
INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');
SELECT * FROM TEXT_TBL;
/*
 * various string functions
 */
select concat('one');
select concat_ws('#','one');
select concat_ws(',',10,20,null,30);
select concat_ws('',10,20,null,30);
select concat_ws(NULL,10,20,null,30) is null;
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
select format('Hello %s', 'World');
select format('Hello %%');
select format('Hello %%%%');
-- should fail
select format('Hello %s %s', 'World');
select format('Hello %s');
select format('Hello %x', 20);
-- check literal and sql identifiers
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, 'Hello');
select format('%s%s%s','Hello', NULL,'World');
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, NULL);
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', NULL, 'Hello');
-- should fail, sql identifier cannot be NULL
select format('INSERT INTO %I VALUES(%L,%L)', NULL, 10, 'Hello');
-- check positional placeholders
select format('%1$s %3$s', 1, 2, 3);
select format('%1$s %12$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
-- should fail
select format('%1$s %4$s', 1, 2, 3);
select format('%1$s %13$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
select format('%0$s', 'Hello');
select format('%*0$s', 'Hello');
select format('%1$', 1);
select format('%1$1', 1);
-- check mix of positional and ordered placeholders
select format('Hello %s %1$s %s', 'World', 'Hello again');
select format('Hello %s %s, %2$s %2$s', 'World', 'Hello again');
-- check field widths and left, right alignment
select format('>>%10s<<', 'Hello');
select format('>>%10s<<', NULL);
select format('>>%10s<<', '');
select format('>>%-10s<<', '');
select format('>>%-10s<<', 'Hello');
select format('>>%-10s<<', NULL);
select format('>>%1$10s<<', 'Hello');
select format('>>%1$-10I<<', 'Hello');
select format('>>%2$*1$L<<', 10, 'Hello');
select format('>>%2$*1$L<<', 10, NULL);
select format('>>%2$*1$L<<', -10, NULL);
select format('>>%*s<<', 10, 'Hello');
select format('>>%*1$s<<', 10, 'Hello');
select format('>>%-s<<', 'Hello');
select format('>>%10L<<', NULL);
select format('>>%2$*1$L<<', NULL, 'Hello');
select format('>>%2$*1$L<<', 0, 'Hello');
