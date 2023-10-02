select
    PgConst('foo', pgvarchar, 2),
    PgConst(12345, pgvarchar, 2),
    PgConst('{foo,bar}', _pgvarchar, 2);