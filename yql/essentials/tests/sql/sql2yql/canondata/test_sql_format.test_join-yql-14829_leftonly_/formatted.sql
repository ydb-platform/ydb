USE plato;

PRAGMA yt.JoinEnableStarJoin = 'true';

INSERT INTO @x
SELECT
    Just('xxx') AS id
ORDER BY
    id
;

INSERT INTO @y
SELECT
    Just('yyy') AS id
ORDER BY
    id
;

INSERT INTO @a
SELECT
    Just('aaa') AS id
ORDER BY
    id
;

COMMIT;

$xy_leftonly = (
    SELECT
        x.id AS id
    FROM
        @x AS x
    LEFT ONLY JOIN
        @y AS y
    ON
        x.id == y.id
);

SELECT
    *
FROM
    @a AS a
LEFT ONLY JOIN
    $xy_leftonly AS b
ON
    a.id == b.id
;
