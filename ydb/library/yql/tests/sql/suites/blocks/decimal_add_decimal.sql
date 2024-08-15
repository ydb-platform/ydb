USE plato;

SELECT
    cs_ext_list_price + cs_ext_tax,
    cs_ext_list_price - cs_ext_tax,
FROM Input;

