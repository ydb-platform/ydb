/* syntax version 1 */
/* postgres can not */
USE plato;

--insert into Output
SELECT
    groupTribit,
    count(*) AS count
FROM Input
GROUP BY
    TableRecordIndex() % 3 AS groupTribit
ORDER BY
    groupTribit,
    count;
