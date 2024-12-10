INSERT INTO plato.Output
SELECT
    StablePickle(TableRow())
FROM
    plato.Input
;
