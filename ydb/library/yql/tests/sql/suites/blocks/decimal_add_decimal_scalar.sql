USE plato;

SELECT
    cs_ext_list_price + cast("10.2" as decimal(7,2)),
    cs_ext_list_price + cast("99999.99" as decimal(7,2)),
FROM Input;

SELECT
    cast("10.2" as decimal(7,2)) + cs_ext_list_price,
    cast("99999.99" as decimal(7,2)) + cs_ext_list_price,
FROM Input;

