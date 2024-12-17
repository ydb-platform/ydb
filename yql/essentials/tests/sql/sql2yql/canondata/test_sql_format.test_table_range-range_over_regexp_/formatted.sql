/* postgres can not */
/* kikimr can not - range not supported */
/* syntax version 1 */
PRAGMA RegexUseRe2 = 'true';

SELECT
    count(*) AS count
FROM
    plato.regexp(``, 'np')
;
