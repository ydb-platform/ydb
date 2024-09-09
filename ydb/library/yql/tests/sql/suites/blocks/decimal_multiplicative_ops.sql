USE plato;

SELECT
    cs_ext_list_price,  cs_ext_tax,
    cs_ext_list_price * cs_ext_tax, cs_ext_list_price * Just(decimal("13.37", 7, 2)), Just(decimal("42.0", 7, 2)) * cs_ext_tax,
    cs_ext_list_price / cs_ext_tax, cs_ext_list_price / Just(decimal("13.37", 7, 2)), Just(decimal("42.0", 7, 2)) / cs_ext_tax,
    cs_ext_list_price % cs_ext_tax, cs_ext_list_price % Just(decimal("13.37", 7, 2)), Just(decimal("42.0", 7, 2)) % cs_ext_tax,
    cs_ext_list_price * cs_ext_tax, cs_ext_list_price * Just(13),
    cs_ext_list_price / cs_ext_tax, cs_ext_list_price / Just(13),
    cs_ext_list_price % cs_ext_tax, cs_ext_list_price % Just(13),
FROM Input;

