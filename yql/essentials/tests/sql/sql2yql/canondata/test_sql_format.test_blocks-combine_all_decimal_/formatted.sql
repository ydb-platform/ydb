USE plato;

SELECT
    count(cs_ext_list_price),
    sum(cs_ext_tax),
    sum(cs_ext_list_price),
    min(cs_sales_price),
    max(cs_ext_list_price),
    avg(cs_ext_tax),
    sum(CAST(cs_sales_price AS float)),
    min(CAST(cs_ext_list_price AS float)),
    max(CAST(cs_ext_tax AS float)),
    avg(CAST(cs_sales_price AS float))
FROM
    Input
;
