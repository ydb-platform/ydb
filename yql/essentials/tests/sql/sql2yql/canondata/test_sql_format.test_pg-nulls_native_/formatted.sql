USE plato;

PRAGMA yt.UseNativeYtTypes;

INSERT INTO @foo
SELECT
    1 AS a,
    Nothing(pgcstring) AS i1,
    Just(Nothing(pgcstring)) AS j1,
    Nothing(pgcstring?) AS j2,
    Just(Just(Nothing(pgcstring))) AS k1,
    Just(Nothing(pgcstring?)) AS k2,
    Nothing(pgcstring??) AS k3
;

COMMIT;

INSERT INTO @bar
SELECT
    t.a + 1 AS a,
    t.*
WITHOUT
    a
FROM
    @foo AS t
;

COMMIT;

SELECT
    a,
    i1,
    i1 IS NULL AS i1n,
    j1,
    j1 IS NULL AS j1n,
    j2,
    j2 IS NULL AS j2n,
    k1,
    k1 IS NULL AS k1n,
    k2,
    k2 IS NULL AS k2n,
    k3,
    k3 IS NULL AS k3n
FROM
    @bar
;
