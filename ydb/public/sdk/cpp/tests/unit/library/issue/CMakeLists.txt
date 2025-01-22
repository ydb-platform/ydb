add_ydb_test(NAME yql-public-issue_ut
  SOURCES
    utf8_ut.cpp
    yql_issue_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    yql-public-issue
    unicode-normalization
  LABELS
    unit
)
