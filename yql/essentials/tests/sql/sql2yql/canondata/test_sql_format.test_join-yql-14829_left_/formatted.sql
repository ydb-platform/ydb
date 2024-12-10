USE plato;

PRAGMA yt.JoinEnableStarJoin = "true";

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

$xy_left = (
    SELECT
        x.id AS id
    FROM ANY
        @x AS x
    LEFT JOIN ANY
        @y AS y
    ON
        x.id == y.id
);

SELECT
    *
FROM ANY
    @a AS a
LEFT JOIN ANY
    $xy_left AS b
ON
    a.id == b.id
;
