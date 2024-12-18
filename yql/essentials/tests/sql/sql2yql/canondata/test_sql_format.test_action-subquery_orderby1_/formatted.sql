/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $sub() AS
    SELECT
        *
    FROM (
        VALUES
            (1),
            (2),
            (3)
    ) AS a (
        x
    );
END DEFINE;

$sub2 = SubqueryOrderBy($sub, [('x', FALSE)]);

PROCESS $sub2();
