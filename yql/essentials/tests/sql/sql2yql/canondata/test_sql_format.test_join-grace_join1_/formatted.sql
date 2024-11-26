USE plato;
PRAGMA DisableSimpleColumns;

SELECT
    cust.customer_id,
    cntr.country_name
FROM plato.countries1
    AS cntr
JOIN plato.customers1
    AS cust
ON cntr.country_id == cust.country_id
WHERE cntr.country_id == "11";
