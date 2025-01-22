add_ydb_test(NAME yql-public-decimal_ut
  SOURCES
    yql_decimal_ut.cpp
    yql_wide_int_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    yql-public-decimal
  LABELS
    unit
)
