/* postgres can not */
USE plato;

INSERT INTO @tmp
SELECT
    Just(
        (
            AsTagged(1, 'A'),
            AsTagged(just(2), 'B'),
            AsTagged(NULL, 'C'),
            AsTagged(Nothing(Int32?), 'D'),
            AsTagged(Nothing(pgint4?), 'E')
        )
    ) AS x
;

COMMIT;

SELECT
    x.0,
    x.1,
    x.2,
    x.3,
    x.4
FROM
    @tmp
;
