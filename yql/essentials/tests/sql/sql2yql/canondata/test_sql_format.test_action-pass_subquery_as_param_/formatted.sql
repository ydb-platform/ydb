/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $dup($x) AS
    SELECT
        *
    FROM
        $x(1)
    UNION ALL
    SELECT
        *
    FROM
        $x(2)
    ;
END DEFINE;

DEFINE SUBQUERY $sub($n) AS
    SELECT
        $n * 10
    ;
END DEFINE;

SELECT
    *
FROM
    $dup($sub)
;
