/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    countIf(kkstritem % 10 == 0) AS count_dec,
    countIf(kkstritem < 100) AS count_small,
    countIf(kkstritem > 900) AS count_huge
FROM (
    SELECT
        keyitem.kk AS kkstr
    FROM Input
        AS d
        FLATTEN BY key AS keyitem
)
    FLATTEN BY kkstr AS kkstritem;
