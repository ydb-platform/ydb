LIBRARY()

IF(OS_WINDOWS)
    CONLYFLAGS(-DWIN32 -DUSE_STDLIB_H)
ELSEIF(OS_DARWIN OR OS_MACOS)
    CONLYFLAGS(-DUSE_STDLIB_H)
    CFLAGS(-DINTERIX)
ELSE()
    CFLAGS(-DLINUX)
ENDIF()

SRCS(
    driver.c
    driver.cpp
    data_generator.cpp
    dg_call_center.cpp
    dg_catalog_sales.cpp
    dg_catalog_page.cpp
    dg_customer.cpp
    dg_customer_address.cpp
    dg_customer_demographics.cpp
    dg_date_dim.cpp
    dg_household_demographics.cpp
    dg_income_band.cpp
    dg_inventory.cpp
    dg_item.cpp
    dg_promotion.cpp
    dg_reason.cpp
    dg_ship_mode.cpp
    dg_store.cpp
    dg_store_sales.cpp
    dg_time_dim.cpp
    dg_warehouse.cpp
    dg_web_page.cpp
    dg_web_sales.cpp
    dg_web_site.cpp
    GLOBAL registrar.cpp
    tpcds.cpp
)

RESOURCE(
    tpcds_schema.yaml tpcds_schema.yaml
)

ALL_RESOURCE_FILES_FROM_DIRS(
    PREFIX tpcds/
    s1_canonical
    s10_canonical
    s100_canonical
)

PEERDIR(
    library/cpp/charset/lite
    library/cpp/resource
    ydb/library/accessor
    ydb/library/workload/tpc_base
    ydb/library/benchmarks/gen/tpcds-dbgen
    ydb/library/benchmarks/queries/tpcds
)

END()
