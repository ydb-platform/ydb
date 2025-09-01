USE plato;

SELECT
    avg(cs_ext_list_price), avg(cs_ext_tax),
    avg(cs_ext_list_price) * decimal("1.1", 7, 2),
    decimal("1.1", 7, 2) * avg(cs_ext_tax),
FROM Input;

