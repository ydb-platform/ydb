USE plato;

SELECT
    cs_ext_list_price + cs_sales_price,
    CAST(1 AS decimal (7, 2)) + cs_ext_list_price,
    cs_sales_price + CAST(2ul AS decimal (7, 2))
FROM
    Input
;
