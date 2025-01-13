/* postgres can not */
SELECT
    ListReplicate(-1, 10),
    ListReplicate(AsTuple(1, 2), 3),
    ListReplicate('foo', 2),
    ListReplicate(TRUE, 0)
;
