/* postgres can not */
/* kikimr can not - range not supported */
/* syntax version 1 */
SELECT
    count(*) AS count
FROM plato.like(``, "_np%");
