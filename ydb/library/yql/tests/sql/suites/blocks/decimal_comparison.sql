USE plato;

SELECT
    cs_ext_list_price == cs_ext_tax,
    cs_ext_list_price < cs_ext_tax,
    cs_ext_list_price <= cs_ext_tax,
    cs_ext_list_price > cs_ext_tax,
    cs_ext_list_price >= cs_ext_tax,

    cs_ext_tax == decimal("26.91", 7, 2),
    cs_ext_tax < decimal("26.91", 7, 2),
    cs_ext_tax <= decimal("26.91", 7, 2),
    cs_ext_tax > decimal("26.91", 7, 2),
    cs_ext_tax >= decimal("26.91", 7, 2),

    decimal("26.91", 7, 2) == cs_ext_tax,
    decimal("26.91", 7, 2) < cs_ext_tax,
    decimal("26.91", 7, 2) <= cs_ext_tax,
    decimal("26.91", 7, 2) > cs_ext_tax,
    decimal("26.91", 7, 2) >= cs_ext_tax,
FROM Input;

