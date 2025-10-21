/* syntax version 1 */
/* postgres can not */
USE plato;

-- These examples are taken from [ISO/IEC TR 19075-6:2017] standard (https://www.iso.org/standard/67367.html)
SELECT T.K
FROM T
WHERE JSON_EXISTS (T.J, 'lax $.where');

SELECT T.K
FROM T
WHERE JSON_EXISTS (T.J, 'strict $.where');

SELECT T.K
FROM T
WHERE JSON_EXISTS(T.J, 'strict $.where' FALSE ON ERROR);

SELECT T.K
FROM T
WHERE JSON_EXISTS (T.J, 'strict $.friends[*].rank');

-- NOTE: Table "T" was renamed to "Filter{index}" to combine several filter examples in 1 test
SELECT K
FROM Filter1
WHERE JSON_EXISTS (Filter1.J, 'lax $ ? (@.pay/@.hours > 9)');

SELECT K
FROM Filter1
WHERE JSON_EXISTS (Filter1.J, 'strict $ ? (@.pay/@.hours > 9)');

SELECT K
FROM Filter2
WHERE JSON_EXISTS (Filter2.J, 'lax $ ? (@.pay/@.hours > 9)');

SELECT K
FROM Filter2
WHERE JSON_EXISTS (Filter2.J, 'strict $ ? (@.pay/@.hours > 9)');

SELECT K
FROM Filter2
WHERE JSON_EXISTS (Filter2.J, 'lax $ ? (@.hours > 9)');

SELECT K
FROM Filter2
WHERE JSON_EXISTS (Filter2.J, 'strict $ ? (@.hours > 9)');

-- NOTE: Standard also provides several examples with is unknown predicate. Following their inimitable style
-- standard authors do not provide data for these examples so we do not include them here
