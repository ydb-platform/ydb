PRAGMA YqlSelect = 'force';

SELECT
    make,
    model,
    Grouping(make, model)
FROM (
    VALUES
        ('Foo', 'GT', 10),
        ('Foo', 'Tour', 20),
        ('Bar', 'City', 15),
        ('Bar', 'Sport', 5)
) AS items_sold (
    make,
    model,
    sales
)
GROUP BY
    ROLLUP (make, model)
;
