PRAGMA AnsiLike;

SELECT
    'abc' LIKE '%%%%%',
    'qqq' LIKE '%q%',
    'qqq' LIKE '%q%q%q%',
    'def' LIKE '%f',
    'def' LIKE '%%%f',
    'foo' LIKE 'foo%foo',
    'foofoo' LIKE 'foo%%foo',
    'abcxzfoo' LIKE '%x_foo',
;
