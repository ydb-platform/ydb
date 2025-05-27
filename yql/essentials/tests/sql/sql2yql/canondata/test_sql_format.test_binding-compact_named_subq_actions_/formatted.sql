PRAGMA CompactNamedExprs;

$src = (
    SELECT
        1
);

DEFINE SUBQUERY $sub1() AS
    SELECT
        *
    FROM
        $src
    ;
END DEFINE;

$foo = 1 + 2;

DEFINE SUBQUERY $sub2($sub, $extra) AS
    SELECT
        a.*,
        $extra AS extra,
        $foo AS another
    FROM
        $sub() AS a
    ;
END DEFINE;

SELECT
    *
FROM
    $sub1()
;

SELECT
    *
FROM
    $sub2($sub1, 1)
;

SELECT
    *
FROM
    $sub2($sub1, 'aaa')
;

DEFINE ACTION $hello_world($sub, $name, $suffix?) AS
    $name = $name ?? ($suffix ?? 'world');

    SELECT
        'Hello, ' || $name || '!'
    FROM
        $sub()
    ;
END DEFINE;

DO
    EMPTY_ACTION()
;

DO
    $hello_world($sub1, NULL)
;

DO
    $hello_world($sub1, NULL, 'John')
;

DO
    $hello_world($sub1, NULL, 'Earth')
;
