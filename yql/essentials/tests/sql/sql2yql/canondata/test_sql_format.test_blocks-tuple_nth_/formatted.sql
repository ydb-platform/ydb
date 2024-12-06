USE plato;

INSERT INTO @tmp
SELECT
    key,
    (subkey, key) AS a,
    (1, key) AS b,
    Just((subkey, key)) AS c,
    Just((Just(subkey), key)) AS d,
    Nothing(Tuple<Int32, Int32>?) AS e,
    Nothing(Tuple<Int32?, Int32>?) AS f,
FROM
    Input
;
COMMIT;

SELECT
    a.0,
    a.1,
    b.0,
    b.1,
    c.0,
    c.1,
    d.0,
    d.1,
    e.0,
    e.1,
    f.0,
    f.1
FROM
    @tmp
;
