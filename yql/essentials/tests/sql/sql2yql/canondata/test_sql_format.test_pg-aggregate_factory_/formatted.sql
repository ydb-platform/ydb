SELECT
    Pg::string_agg(x, ','p)
FROM (
    VALUES
        ('a'p),
        ('b'p),
        ('c'p)
) AS a (
    x
);

SELECT
    Pg::string_agg(x, ','p) OVER (
        ORDER BY
            x
    ),
FROM (
    VALUES
        ('a'p),
        ('b'p),
        ('c'p)
) AS a (
    x
);

$agg_string_agg = AggregationFactory('Pg::string_agg');

SELECT
    AggregateBy((x, ','p), $agg_string_agg)
FROM (
    VALUES
        ('a'p),
        ('b'p),
        ('c'p)
) AS a (
    x
);

SELECT
    AggregateBy((x, ','p), $agg_string_agg) OVER (
        ORDER BY
            x
    ),
FROM (
    VALUES
        ('a'p),
        ('b'p),
        ('c'p)
) AS a (
    x
);

$agg_max = AggregationFactory('Pg::max');

SELECT
    AggregateBy(x, $agg_max)
FROM (
    VALUES
        ('a'p),
        ('b'p),
        ('c'p)
) AS a (
    x
);

SELECT
    AggregateBy(x, $agg_max) OVER (
        ORDER BY
            x
    ),
FROM (
    VALUES
        ('a'p),
        ('b'p),
        ('c'p)
) AS a (
    x
);
