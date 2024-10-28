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
    tpcds_schema.sql tpcds_schema.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q1.sql tpcds/yql/q1.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q2.sql tpcds/yql/q2.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q3.sql tpcds/yql/q3.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q4.sql tpcds/yql/q4.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q5.sql tpcds/yql/q5.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q6.sql tpcds/yql/q6.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q7.sql tpcds/yql/q7.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q8.sql tpcds/yql/q8.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q9.sql tpcds/yql/q9.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q10.sql tpcds/yql/q10.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q11.sql tpcds/yql/q11.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q12.sql tpcds/yql/q12.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q13.sql tpcds/yql/q13.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q14.sql tpcds/yql/q14.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q15.sql tpcds/yql/q15.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q16.sql tpcds/yql/q16.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q17.sql tpcds/yql/q17.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q18.sql tpcds/yql/q18.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q19.sql tpcds/yql/q19.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q20.sql tpcds/yql/q20.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q21.sql tpcds/yql/q21.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q22.sql tpcds/yql/q22.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q23.sql tpcds/yql/q23.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q24.sql tpcds/yql/q24.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q25.sql tpcds/yql/q25.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q26.sql tpcds/yql/q26.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q27.sql tpcds/yql/q27.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q28.sql tpcds/yql/q28.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q29.sql tpcds/yql/q29.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q30.sql tpcds/yql/q30.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q31.sql tpcds/yql/q31.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q32.sql tpcds/yql/q32.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q33.sql tpcds/yql/q33.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q34.sql tpcds/yql/q34.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q35.sql tpcds/yql/q35.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q36.sql tpcds/yql/q36.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q37.sql tpcds/yql/q37.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q38.sql tpcds/yql/q38.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q39.sql tpcds/yql/q39.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q40.sql tpcds/yql/q40.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q41.sql tpcds/yql/q41.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q42.sql tpcds/yql/q42.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q43.sql tpcds/yql/q43.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q44.sql tpcds/yql/q44.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q45.sql tpcds/yql/q45.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q46.sql tpcds/yql/q46.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q47.sql tpcds/yql/q47.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q48.sql tpcds/yql/q48.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q49.sql tpcds/yql/q49.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q50.sql tpcds/yql/q50.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q51.sql tpcds/yql/q51.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q52.sql tpcds/yql/q52.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q53.sql tpcds/yql/q53.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q54.sql tpcds/yql/q54.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q55.sql tpcds/yql/q55.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q56.sql tpcds/yql/q56.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q57.sql tpcds/yql/q57.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q58.sql tpcds/yql/q58.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q59.sql tpcds/yql/q59.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q60.sql tpcds/yql/q60.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q61.sql tpcds/yql/q61.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q62.sql tpcds/yql/q62.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q63.sql tpcds/yql/q63.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q64.sql tpcds/yql/q64.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q65.sql tpcds/yql/q65.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q66.sql tpcds/yql/q66.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q67.sql tpcds/yql/q67.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q68.sql tpcds/yql/q68.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q69.sql tpcds/yql/q69.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q70.sql tpcds/yql/q70.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q71.sql tpcds/yql/q71.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q72.sql tpcds/yql/q72.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q73.sql tpcds/yql/q73.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q74.sql tpcds/yql/q74.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q75.sql tpcds/yql/q75.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q76.sql tpcds/yql/q76.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q77.sql tpcds/yql/q77.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q78.sql tpcds/yql/q78.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q79.sql tpcds/yql/q79.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q80.sql tpcds/yql/q80.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q81.sql tpcds/yql/q81.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q82.sql tpcds/yql/q82.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q83.sql tpcds/yql/q83.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q84.sql tpcds/yql/q84.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q85.sql tpcds/yql/q85.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q86.sql tpcds/yql/q86.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q87.sql tpcds/yql/q87.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q88.sql tpcds/yql/q88.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q89.sql tpcds/yql/q89.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q90.sql tpcds/yql/q90.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q91.sql tpcds/yql/q91.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q92.sql tpcds/yql/q92.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q93.sql tpcds/yql/q93.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q94.sql tpcds/yql/q94.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q95.sql tpcds/yql/q95.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q96.sql tpcds/yql/q96.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q97.sql tpcds/yql/q97.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q98.sql tpcds/yql/q98.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q99.sql tpcds/yql/q99.sql

    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q01.sql tpcds/pg/q1.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q02.sql tpcds/pg/q2.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q03.sql tpcds/pg/q3.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q04.sql tpcds/pg/q4.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q05.sql tpcds/pg/q5.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q06.sql tpcds/pg/q6.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q07.sql tpcds/pg/q7.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q08.sql tpcds/pg/q8.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q09.sql tpcds/pg/q9.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q10.sql tpcds/pg/q10.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q11.sql tpcds/pg/q11.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q12.sql tpcds/pg/q12.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q13.sql tpcds/pg/q13.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q14.sql tpcds/pg/q14.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q15.sql tpcds/pg/q15.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q16.sql tpcds/pg/q16.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q17.sql tpcds/pg/q17.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q18.sql tpcds/pg/q18.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q19.sql tpcds/pg/q19.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q20.sql tpcds/pg/q20.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q21.sql tpcds/pg/q21.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q22.sql tpcds/pg/q22.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q23.sql tpcds/pg/q23.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q24.sql tpcds/pg/q24.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q25.sql tpcds/pg/q25.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q26.sql tpcds/pg/q26.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q27.sql tpcds/pg/q27.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q28.sql tpcds/pg/q28.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q29.sql tpcds/pg/q29.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q30.sql tpcds/pg/q30.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q31.sql tpcds/pg/q31.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q32.sql tpcds/pg/q32.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q33.sql tpcds/pg/q33.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q34.sql tpcds/pg/q34.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q35.sql tpcds/pg/q35.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q36.sql tpcds/pg/q36.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q37.sql tpcds/pg/q37.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q38.sql tpcds/pg/q38.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q39.sql tpcds/pg/q39.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q40.sql tpcds/pg/q40.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q41.sql tpcds/pg/q41.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q42.sql tpcds/pg/q42.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q43.sql tpcds/pg/q43.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q44.sql tpcds/pg/q44.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q45.sql tpcds/pg/q45.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q46.sql tpcds/pg/q46.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q47.sql tpcds/pg/q47.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q48.sql tpcds/pg/q48.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q49.sql tpcds/pg/q49.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q50.sql tpcds/pg/q50.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q51.sql tpcds/pg/q51.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q52.sql tpcds/pg/q52.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q53.sql tpcds/pg/q53.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q54.sql tpcds/pg/q54.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q55.sql tpcds/pg/q55.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q56.sql tpcds/pg/q56.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q57.sql tpcds/pg/q57.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q58.sql tpcds/pg/q58.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q59.sql tpcds/pg/q59.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q60.sql tpcds/pg/q60.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q61.sql tpcds/pg/q61.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q62.sql tpcds/pg/q62.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q63.sql tpcds/pg/q63.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q64.sql tpcds/pg/q64.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q65.sql tpcds/pg/q65.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q66.sql tpcds/pg/q66.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q67.sql tpcds/pg/q67.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q68.sql tpcds/pg/q68.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q69.sql tpcds/pg/q69.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q70.sql tpcds/pg/q70.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q71.sql tpcds/pg/q71.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q72.sql tpcds/pg/q72.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q73.sql tpcds/pg/q73.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q74.sql tpcds/pg/q74.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q75.sql tpcds/pg/q75.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q76.sql tpcds/pg/q76.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q77.sql tpcds/pg/q77.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q78.sql tpcds/pg/q78.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q79.sql tpcds/pg/q79.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q80.sql tpcds/pg/q80.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q81.sql tpcds/pg/q81.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q82.sql tpcds/pg/q82.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q83.sql tpcds/pg/q83.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q84.sql tpcds/pg/q84.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q85.sql tpcds/pg/q85.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q86.sql tpcds/pg/q86.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q87.sql tpcds/pg/q87.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q88.sql tpcds/pg/q88.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q89.sql tpcds/pg/q89.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q90.sql tpcds/pg/q90.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q91.sql tpcds/pg/q91.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q92.sql tpcds/pg/q92.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q93.sql tpcds/pg/q93.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q94.sql tpcds/pg/q94.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q95.sql tpcds/pg/q95.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q96.sql tpcds/pg/q96.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q97.sql tpcds/pg/q97.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q98.sql tpcds/pg/q98.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/pg/q99.sql tpcds/pg/q99.sql
)

PEERDIR(
    library/cpp/charset
    library/cpp/resource
    ydb/library/accessor
    ydb/library/workload/tpc_base
    ydb/library/benchmarks/gen/tpcds-dbgen
)

END()
