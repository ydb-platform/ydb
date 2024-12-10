USE plato;

SELECT
    cs_ext_list_price + CAST("10.2" AS decimal (7, 2)),
    cs_ext_list_price + CAST("99999.99" AS decimal (7, 2)),
    cs_ext_list_price - CAST("11.22" AS decimal (7, 2)),
    cs_ext_list_price - CAST("99999.99" AS decimal (7, 2)),
FROM
    Input
;

SELECT
    CAST("10.2" AS decimal (7, 2)) + cs_ext_list_price,
    CAST("99999.99" AS decimal (7, 2)) + cs_ext_list_price,
    CAST("11.22" AS decimal (7, 2)) - cs_ext_list_price,
    CAST("99999.99" AS decimal (7, 2)) - cs_ext_list_price,
FROM
    Input
;
