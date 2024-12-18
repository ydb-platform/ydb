/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $sub() AS
    SELECT
        *
    FROM (
        VALUES
            (1, 'c'),
            (1, 'a'),
            (3, 'b')
    ) AS a (
        x,
        y
    );
END DEFINE;

$sub2 = SubqueryOrderBy($sub, [('x', FALSE), ('y', TRUE)]);

PROCESS $sub2();
