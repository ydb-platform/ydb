USE plato;

PRAGMA yt.MapJoinLimit = "1m";

$t1 = AsList(
    AsStruct(Just(1) AS Key),
    AsStruct(Just(2) AS Key),
    AsStruct(Just(3) AS Key)
);

$t2 = AsList(
    AsStruct(Just(Just(2)) AS Key),
    AsStruct(Just(Just(3)) AS Key),
    AsStruct(Just(Just(4)) AS Key),
    AsStruct(Just(Just(5)) AS Key),
    AsStruct(Just(Just(6)) AS Key)
);

INSERT INTO @t1
SELECT
    *
FROM
    as_table($t1)
;

INSERT INTO @t2
SELECT
    *
FROM
    as_table($t2)
;

COMMIT;

SELECT
    *
FROM
    @t1 AS a
JOIN
    @t2 AS b
USING (Key);
