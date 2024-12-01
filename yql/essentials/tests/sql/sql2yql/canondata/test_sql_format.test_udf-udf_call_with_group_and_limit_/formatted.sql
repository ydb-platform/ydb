/* postgres can not */
SELECT
    SimpleUdf::Echo(key) AS key,
    count(*) AS count
FROM plato.Input
GROUP BY
    key
ORDER BY
    key /* sort for stable results only */
LIMIT 2;
