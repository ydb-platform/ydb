USE plato;

PRAGMA DisableSimpleColumns;

SELECT
    c1.customer_id,
    c2.customer_id
FROM
    plato.customers1 AS c1
JOIN
    plato.customers1 AS c2
ON
    c1.country_id == c2.country_id
ORDER BY
    c1.customer_id,
    c2.customer_id
;
