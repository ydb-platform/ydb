add_ydb_test(NAME yql-utils_ut
  SOURCES
    utils/utf8_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    yql-utils
  LABELS
    unit
)

add_ydb_test(NAME yql-public-issue_ut
  SOURCES
    issue/yql_issue_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    yql-public-issue
    unicode-normalization
  LABELS
    unit
)

add_ydb_test(NAME yql-public-decimal_ut
  SOURCES
    decimal/yql_decimal_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    yql-public-decimal
  LABELS
    unit
)

add_ydb_test(NAME yql-public-wide_int_ut
  SOURCES
    decimal/yql_wide_int_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    yql-public-decimal
  LABELS
    unit
)
