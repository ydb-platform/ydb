/* postgres can not */
PRAGMA library('multiaggr_subq.sql');
PRAGMA library('agg_factory.sql');

IMPORT multiaggr_subq SYMBOLS $multiaggr_win;

SELECT
    *
FROM
    $multiaggr_win()
ORDER BY
    rn
;
