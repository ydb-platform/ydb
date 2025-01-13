/* syntax version 1 */
DECLARE $foo AS List<Int32>;

SELECT
    1 IN $foo,
    100 IN $foo
;
