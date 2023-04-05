--
-- TEXT
--

SELECT text 'this is a text string' = text 'this is a text string' AS true;

SELECT text 'this is a text string' = text 'this is a text strin' AS false;

/*
 * various string functions
 */
select reverse('abcde');
select quote_literal('');
select quote_literal('abc''');
select quote_literal(e'\\');
