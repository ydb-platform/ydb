LIBRARY()

CONLYFLAGS(
    -Wno-format
    -Wno-misleading-indentation
    -Wno-missing-field-initializers
    -Wno-unused-parameter
    -Wno-unused-variable
    -Wno-unused-but-set-variable
)

CONLYFLAGS(GLOBAL -DLINUX)
NO_WERROR()

SRCS(
    address.c
    build_support.c
    date.c
    decimal.c
#    dist.c
#    driver.c
    error_msg.c
    genrand.c
    join.c
    list.c
#    load.c
    misc.c
    nulls.c
    parallel.c
    permute.c
    pricing.c
    print.c
    r_params.c
    StringBuffer.c
    tdef_functions.c
    tdefs.c
    text.c
    scd.c
    scaling.c
    release.c
    sparse.c
    validate.c
    s_brand.c
#    s_customer_address.c
    s_call_center.c
    s_catalog.c
    s_catalog_order.c
    s_catalog_order_lineitem.c
    s_catalog_page.c
    s_catalog_promotional_item.c
    s_catalog_returns.c
    s_category.c
    s_class.c
    s_company.c
#    s_customer.c
    s_division.c
    s_inventory.c
    s_item.c
    s_manager.c
    s_manufacturer.c
    s_market.c
    s_pline.c
    s_product.c
    s_promotion.c
    s_purchase.c
    s_reason.c
    s_store.c
    s_store_promotional_item.c
    s_store_returns.c
    s_subcategory.c
    s_subclass.c
    s_warehouse.c
    s_web_order.c
    s_web_order_lineitem.c
    s_web_page.c
    s_web_promotinal_item.c
    s_web_returns.c
    s_web_site.c
    s_zip_to_gmt.c
    w_call_center.c
    w_catalog_page.c
    w_catalog_returns.c
    w_catalog_sales.c
    w_customer_address.c
    w_customer.c
    w_customer_demographics.c
    w_datetbl.c
    w_household_demographics.c
    w_income_band.c
    w_inventory.c
    w_item.c
    w_promotion.c
    w_reason.c
    w_ship_mode.c
    w_store.c
    w_store_returns.c
    w_store_sales.c
    w_timetbl.c
    w_warehouse.c
    w_web_page.c
    w_web_returns.c
    w_web_sales.c
    w_web_site.c
    dbgen_version.c
)

RESOURCE(tpcds.idx tpcds.idx)

END()
