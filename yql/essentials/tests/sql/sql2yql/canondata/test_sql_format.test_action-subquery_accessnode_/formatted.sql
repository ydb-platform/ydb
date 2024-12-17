/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $foo() AS
    SELECT
        <|a: 1, b: 2|> AS s
    ;
END DEFINE;

SELECT
    s.a AS a,
    s.b AS b
FROM
    $foo()
;
