PRAGMA BlockEngine = 'force';

SELECT
    count(key)
FROM
    plato.Input
GROUP BY
    key
;
