/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $a() AS
    $_x = (
        SELECT
            1
    );

    DISCARD SELECT
        ensure(1, TRUE)
    ;

    SELECT
        2
    ;

    $_y = (
        SELECT
            2
    );
END DEFINE;

PROCESS $a();

DEFINE SUBQUERY $b() AS
    $f1 = ($row) -> (<|a: 1, b: $row.value|>);
    $f2 = ($row) -> (<|a: 2, b: $row.value|>);

    DISCARD PROCESS plato.Input
    USING $f1(TableRow());

    PROCESS plato.Input
    USING $f2(TableRow());
END DEFINE;

SELECT
    *
FROM
    $b()
ORDER BY
    b
LIMIT 1;

DEFINE SUBQUERY $c() AS
    $f1 = ($key, $_) -> (<|a: 1, b: $key|>);
    $f2 = ($key, $_) -> (<|a: 2, b: $key|>);

    DISCARD REDUCE plato.Input
    ON
        key
    USING $f1(TableRow());

    REDUCE plato.Input
    ON
        key
    USING $f2(TableRow());
END DEFINE;

SELECT
    *
FROM
    $c()
ORDER BY
    b
LIMIT 1;
