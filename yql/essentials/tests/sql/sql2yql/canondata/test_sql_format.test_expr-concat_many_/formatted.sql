SELECT
    concat(NULL),
    concat('aa'),
    concat('aa', 'bb', 'cc'),
    concat(just('aa'u), just('bb')),
    concat('aa', 'bb'u),
    concat('aa'u, just('bb'u))
;
