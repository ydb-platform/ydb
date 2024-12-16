USE plato;

PRAGMA yt.JoinCollectColumnarStatistics = 'async';

INSERT INTO @yang_ids
SELECT
    *
FROM
    Input
WHERE
    subkey <= '3'
LIMIT 100;

COMMIT;

INSERT INTO @yang_ids
SELECT
    *
FROM
    Input AS j
LEFT ONLY JOIN
    @yang_ids
USING (key);

COMMIT;

INSERT INTO @yang_ids
SELECT
    *
FROM
    Input AS j
LEFT ONLY JOIN
    @yang_ids
USING (key);

COMMIT;

SELECT
    *
FROM
    @yang_ids
;
