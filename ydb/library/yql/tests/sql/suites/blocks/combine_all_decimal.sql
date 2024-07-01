use plato;

SELECT
    count(cs_ext_list_price),
    sum(cs_ext_tax),
    min(cs_sales_price),
    max(cs_ext_list_price),
    avg(cs_ext_tax),

    sum(cast(cs_sales_price as float)),
    min(cast(cs_ext_list_price as float)),
    max(cast(cs_ext_tax as float)),
    avg(cast(cs_sales_price as float))
FROM Input
